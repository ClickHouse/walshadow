#!/usr/bin/env bash
# Shared helpers for the walshadow EC2 node provisioners. Source this AFTER
# aws-env.sh. All helpers echo their result so callers can capture it:
#   AMI=$(latest_ubuntu_ami); SUBNET=$(pick_public_subnet "$VPC_ID" "$TYPE")
#
# The walshadow demo runs three single-node EC2 hosts in one VPC
# (vpc-095cae9ec7f26f611 / 10.0.0.0/16, ap-south-1): source Postgres,
# ClickHouse, and the walshadow daemon. They reach each other over the VPC
# CIDR; each security group additionally allows the operator's egress IP.

# Shared network defaults (override via env before sourcing).
VPC_ID="${VPC_ID:-vpc-095cae9ec7f26f611}"
VPC_CIDR="${VPC_CIDR:-10.0.0.0/16}"

# Operator's current public IP as a /32, for SG rules.
my_ip_cidr() { echo "$(curl -s https://checkip.amazonaws.com)/32"; }

# Latest Ubuntu 24.04 LTS amd64 (Canonical, owner 099720109477).
latest_ubuntu_ami() {
  aws ec2 describe-images --owners 099720109477 \
    --filters 'Name=name,Values=ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*' \
              'Name=state,Values=available' \
    --query 'reverse(sort_by(Images,&CreationDate))[:1].ImageId' --output text
}

# A *public* subnet (route table has 0.0.0.0/0 -> igw) in $1=VPC whose AZ
# offers $2=instance-type. These subnets don't auto-assign a public IP, so
# the launch must pass --associate-public-ip-address.
#
# Selection is deterministic: candidates are sorted by AZ, so the lowest AZ
# (e.g. ap-south-1a) is chosen and all nodes land in the same AZ (avoids
# cross-AZ latency). AWS does NOT guarantee a stable order from
# describe-route-tables, so the sort is what keeps this reproducible.
# Set PREFER_AZ (e.g. ap-south-1a) to pin a specific AZ.
pick_public_subnet() {
  local vpc="$1" itype="$2" offer_azs public rows az sid chosen=""
  offer_azs=",$(aws ec2 describe-instance-type-offerings --location-type availability-zone \
    --filters "Name=instance-type,Values=$itype" \
    --query 'InstanceTypeOfferings[].Location' --output text | tr '\t' ','),"
  public="$(aws ec2 describe-route-tables --filters "Name=vpc-id,Values=$vpc" \
    --query "RouteTables[?Routes[?GatewayId!=null && starts_with(GatewayId,'igw-')]].Associations[].SubnetId" \
    --output text)"
  [ -n "$public" ] || { echo "no igw-routed (public) subnet in $vpc" >&2; return 1; }
  rows="$(aws ec2 describe-subnets --subnet-ids $public \
    --query 'Subnets[].[AvailabilityZone,SubnetId]' --output text | sort)"
  while read -r az sid; do
    [ -n "$az" ] || continue
    case "$offer_azs" in *",$az,"*) : ;; *) continue;; esac
    if [ -n "${PREFER_AZ:-}" ]; then
      [ "$az" = "$PREFER_AZ" ] && { chosen="$sid"; break; }
    else
      chosen="$sid"; break
    fi
  done <<EOF
$rows
EOF
  [ -n "$chosen" ] || { echo "no public subnet in an AZ offering $itype${PREFER_AZ:+ (PREFER_AZ=$PREFER_AZ)}" >&2; return 1; }
  echo "$chosen"
}

# Create key pair $1 if absent, writing the private key to $2 (chmod 600).
ensure_keypair() {
  local name="$1" pem="$2"
  if aws ec2 describe-key-pairs --key-names "$name" >/dev/null 2>&1; then
    echo "key pair $name exists (reusing $pem)" >&2
  else
    aws ec2 create-key-pair --key-name "$name" --query KeyMaterial --output text > "$pem"
    chmod 600 "$pem"
    echo "created key pair -> $pem" >&2
  fi
}

# Echo the id of SG named $1 in $2=VPC, creating it if absent. $3=description.
ensure_sg() {
  local name="$1" vpc="$2" desc="$3" sg
  sg="$(aws ec2 describe-security-groups \
    --filters "Name=group-name,Values=$name" "Name=vpc-id,Values=$vpc" \
    --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || true)"
  if [ -z "$sg" ] || [ "$sg" = "None" ]; then
    sg="$(aws ec2 create-security-group --group-name "$name" \
      --description "$desc" --vpc-id "$vpc" --query GroupId --output text)"
    echo "created SG $sg" >&2
  else
    echo "SG $sg exists (reusing)" >&2
  fi
  echo "$sg"
}

# Idempotently allow tcp $3=port from $2=cidr on $1=sg (ignores Duplicate).
authorize() {
  local sg="$1" cidr="$2" port="$3"
  aws ec2 authorize-security-group-ingress --group-id "$sg" \
    --ip-permissions "IpProtocol=tcp,FromPort=$port,ToPort=$port,IpRanges=[{CidrIp=$cidr}]" \
    >/dev/null 2>&1 && echo "  +$port <- $cidr" >&2 || true
}

# Echo the InstanceId of a running/pending instance tagged Name=$1, or "".
find_running_instance() {
  local iid
  iid="$(aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=$1" "Name=instance-state-name,Values=pending,running" \
    --query 'Reservations[].Instances[0].InstanceId' --output text)"
  [ "$iid" = "None" ] && iid=""
  echo "$iid"
}

# Echo private/public IP of instance $1.
instance_private_ip() { aws ec2 describe-instances --instance-ids "$1" \
  --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text; }
instance_public_ip()  { aws ec2 describe-instances --instance-ids "$1" \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text; }

# ---------------------------------------------------------------------------
# state.env helpers
# ---------------------------------------------------------------------------

# Echo KEY=value's value from a state.env-style file. $1=path, $2=key.
read_state_var() { grep -E "^$2=" "$1" 2>/dev/null | tail -1 | cut -d= -f2-; }

# Echo a peer node's value, erroring if absent. $1=node dir (e.g.
# ../ec2-source-pg), $2=key (PRIVATE_IP / SOURCE_PRIVATE_IP). For deploy.sh.
peer_state_var() {
  local v; v="$(read_state_var "$1/state.env" "$2")"
  [ -n "$v" ] || { echo "missing $2 in $1/state.env — provision that node first" >&2; return 1; }
  echo "$v"
}

# ---------------------------------------------------------------------------
# Generic provision / teardown — driven by a node's ./node.env (NODE_NAME,
# SG_DESC, INGRESS, and optional KEY_NAME/INSTANCE_TYPE/VOLUME_SIZE/
# STATE_PRIVATE_ALIASES/NODE_NEXT). Each node's provision.sh/teardown.sh is a
# thin shim that sources aws-env.sh + lib.sh + node.env, then calls these.
# ---------------------------------------------------------------------------

# Launch (idempotently) the node described by the sourced node.env. Run from
# the node dir (cloud-init.yaml is read as file://cloud-init.yaml).
provision_node() {
  local name="${NODE_NAME:?node.env must set NODE_NAME}"
  local key="${KEY_NAME:-$name}"
  local itype="${INSTANCE_TYPE:-c8i.2xlarge}"
  local vol="${VOLUME_SIZE:-100}"
  local pem="./${key}.pem"
  local state="./state.env"

  local MYIP AMI SUBNET
  MYIP="$(my_ip_cidr)";                              echo "egress IP: $MYIP"
  AMI="$(latest_ubuntu_ami)";                        echo "AMI: $AMI"
  SUBNET="$(pick_public_subnet "$VPC_ID" "$itype")"; echo "subnet: $SUBNET"

  ensure_keypair "$key" "$pem"
  local SG; SG="$(ensure_sg "$name" "$VPC_ID" "${SG_DESC:-$name}")"
  # INGRESS is space-separated "port:scope[,scope]"; scope is my (egress /32),
  # vpc (the VPC CIDR), or a literal CIDR. Defaults to SSH from my IP.
  local rule port scopes s cidr
  for rule in ${INGRESS:-22:my}; do
    port="${rule%%:*}"; scopes="${rule#*:}"
    local -a scope_arr; IFS=',' read -ra scope_arr <<<"$scopes"
    for s in "${scope_arr[@]}"; do
      case "$s" in my) cidr="$MYIP";; vpc) cidr="$VPC_CIDR";; *) cidr="$s";; esac
      authorize "$SG" "$cidr" "$port"
    done
  done

  local IID; IID="$(find_running_instance "$name")"
  if [ -n "$IID" ]; then
    echo "instance already running: $IID (skipping launch)"
  else
    IID="$(aws ec2 run-instances \
      --image-id "$AMI" --instance-type "$itype" \
      --key-name "$key" --security-group-ids "$SG" --subnet-id "$SUBNET" \
      --associate-public-ip-address \
      --block-device-mappings "DeviceName=/dev/sda1,Ebs={VolumeSize=$vol,VolumeType=gp3}" \
      --user-data "file://cloud-init.yaml" \
      --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$name}]" \
      --query 'Instances[0].InstanceId' --output text)"
    echo "launched $IID"
  fi

  echo "waiting for running state..."
  aws ec2 wait instance-running --instance-ids "$IID"
  local PUBIP PRIVIP; PUBIP="$(instance_public_ip "$IID")"; PRIVIP="$(instance_private_ip "$IID")"

  {
    echo "INSTANCE_ID=$IID"
    echo "PUBLIC_IP=$PUBIP"
    echo "PRIVATE_IP=$PRIVIP"
    # Extra keys that should carry the private IP (e.g. source-pg's
    # SOURCE_PRIVATE_IP, which deploy.sh and the bench read).
    local alias; for alias in ${STATE_PRIVATE_ALIASES:-}; do echo "$alias=$PRIVIP"; done
    echo "SG_ID=$SG"
    echo "KEY_NAME=$key"
    echo "REGION=$AWS_DEFAULT_REGION"
  } > "$state"

  echo
  echo "=== ready ==="
  echo "instance:  $IID"
  echo "public IP: $PUBIP   private IP: $PRIVIP"
  echo "ssh:       ssh -i $pem ubuntu@$PUBIP   # (cloud-init takes ~2-3 min)"
  [ -n "${NODE_NEXT:-}" ] && echo "$NODE_NEXT"
  echo "state written to $state"
}

# Copy on-CPU profiles (from ./profile.sh) off the box into ./profiles/<ts>/
# BEFORE termination. Uses KEY_NAME + PUBLIC_IP from the sourced state.env.
copy_remote_profiles() {
  local pem="./${KEY_NAME:-}.pem"
  [ -n "${PUBLIC_IP:-}" ] && [ -f "$pem" ] || return 0
  local ssh_p=(ssh -i "$pem" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 "ubuntu@$PUBLIC_IP")
  "${ssh_p[@]}" 'ls /opt/profile/* >/dev/null 2>&1' || return 0
  local dest="./profiles/$(date +%Y%m%d-%H%M%S)"
  mkdir -p "$dest"
  echo "copying /opt/profile → $dest …"
  scp -i "$pem" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 \
    "ubuntu@$PUBLIC_IP:/opt/profile/*" "$dest/" 2>/dev/null \
    && echo "  copied: $(ls -1 "$dest" 2>/dev/null | tr '\n' ' ')" \
    || echo "  (nothing copied — capture may still be running; re-run teardown after it finishes)"
}

# Terminate the node (from its state.env) and delete its SG. Runs an optional
# node_pre_teardown hook first if the shim defines one (e.g. copy profiles,
# drop a replication slot on the source). Run from the node dir.
teardown_node() {
  [ -f ./state.env ] && source ./state.env
  declare -F node_pre_teardown >/dev/null && node_pre_teardown
  if [ -n "${INSTANCE_ID:-}" ]; then
    echo "terminating $INSTANCE_ID"
    aws ec2 terminate-instances --instance-ids "$INSTANCE_ID" >/dev/null
    aws ec2 wait instance-terminated --instance-ids "$INSTANCE_ID"
    echo "terminated"
  fi
  if [ -n "${SG_ID:-}" ]; then
    # SG can't be deleted until its ENI is gone; the terminate-wait handles that.
    aws ec2 delete-security-group --group-id "$SG_ID" 2>/dev/null \
      && echo "deleted SG $SG_ID" || echo "SG $SG_ID not deleted (may still be in use)"
  fi
  rm -f ./state.env
  echo "done"
}

# deploy.sh preamble helper: set PEM + SSH/SCP arrays from the sourced
# state.env (KEY_NAME, PUBLIC_IP). Populates globals PEM, SSH, SCP.
node_ssh_setup() {
  PEM="./${KEY_NAME}.pem"
  SSH=(ssh -i "$PEM" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15 "ubuntu@$PUBLIC_IP")
  SCP=(scp -i "$PEM" -o StrictHostKeyChecking=accept-new)
}

# Block until cloud-init has finished on the node (SSH must be set up).
wait_cloud_init() {
  echo "waiting for SSH + cloud-init…"
  "${SSH[@]}" 'sudo cloud-init status --wait' || { echo "cloud-init did not finish cleanly" >&2; return 1; }
}
