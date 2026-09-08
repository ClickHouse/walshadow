#!/usr/bin/env bash
#
# Provision the demo VM: dedicated VPC, restricted SSH, public HTTP/S.
#
# The account is shared and full of managed ClickHouse Cloud VPCs, so this
# builds its own network rather than borrowing one. Re-running is safe: every
# resource is looked up by tag first.
#
#   ./provision.sh              provision (or report existing)
#   ./provision.sh destroy      remove everything it created
#
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

NAME=openhouse-demo
REGION="${AWS_DEFAULT_REGION:-us-east-1}"
CIDR=10.180.0.0/16
SUBNET_CIDR=10.180.1.0/24
INSTANCE_TYPE="${INSTANCE_TYPE:-c7i.2xlarge}"
PEM="$PWD/$NAME.pem"
STATE="$PWD/state.env"

: "${AWS_ACCESS_KEY_ID:?export AWS credentials first}"
export AWS_SHARED_CREDENTIALS_FILE=/dev/null AWS_CONFIG_FILE=/dev/null
export AWS_DEFAULT_REGION="$REGION"

tagged() { # tagged <resource-type> <query>; empty when absent
  local out
  out=$(aws ec2 describe-"$1" --filters "Name=tag:Name,Values=$NAME" \
    --query "$2" --output text 2>/dev/null || true)
  [ "$out" = None ] && out=""
  printf '%s' "$out"
}
tag() { aws ec2 create-tags --resources "$1" --tags "Key=Name,Value=$NAME" >/dev/null; }

destroy() {
  [ -f "$STATE" ] && . "$STATE"
  echo "==> terminating instance"
  [ -n "${INSTANCE_ID:-}" ] && aws ec2 terminate-instances --instance-ids "$INSTANCE_ID" >/dev/null 2>&1 || true
  [ -n "${INSTANCE_ID:-}" ] && aws ec2 wait instance-terminated --instance-ids "$INSTANCE_ID" 2>/dev/null || true
  for f in "aws ec2 delete-security-group --group-id ${SG_ID:-}" \
           "aws ec2 delete-subnet --subnet-id ${SUBNET_ID:-}" \
           "aws ec2 delete-route-table --route-table-id ${RTB_ID:-}" \
           "aws ec2 detach-internet-gateway --internet-gateway-id ${IGW_ID:-} --vpc-id ${VPC_ID:-}" \
           "aws ec2 delete-internet-gateway --internet-gateway-id ${IGW_ID:-}" \
           "aws ec2 delete-vpc --vpc-id ${VPC_ID:-}" \
           "aws ec2 delete-key-pair --key-name $NAME"; do
    echo "  $f"; $f >/dev/null 2>&1 || true
  done
  rm -f "$STATE" "$PEM"
  echo "✅ destroyed"
}
[ "${1:-}" = destroy ] && { destroy; exit 0; }

MY_IP="$(curl -s https://checkip.amazonaws.com | tr -d '\n')/32"
echo "==> operator IP: $MY_IP"

VPC_ID=$(tagged vpcs 'Vpcs[0].VpcId')
if [ -z "$VPC_ID" ]; then
  VPC_ID=$(aws ec2 create-vpc --cidr-block "$CIDR" --query Vpc.VpcId --output text)
  tag "$VPC_ID"
  aws ec2 modify-vpc-attribute --vpc-id "$VPC_ID" --enable-dns-hostnames >/dev/null
  echo "==> created vpc $VPC_ID"
else echo "==> reusing vpc $VPC_ID"; fi

IGW_ID=$(tagged internet-gateways 'InternetGateways[0].InternetGatewayId')
if [ -z "$IGW_ID" ]; then
  IGW_ID=$(aws ec2 create-internet-gateway --query InternetGateway.InternetGatewayId --output text)
  tag "$IGW_ID"
  aws ec2 attach-internet-gateway --internet-gateway-id "$IGW_ID" --vpc-id "$VPC_ID" >/dev/null
  echo "==> created igw $IGW_ID"
fi

SUBNET_ID=$(tagged subnets 'Subnets[0].SubnetId')
if [ -z "$SUBNET_ID" ]; then
  SUBNET_ID=$(aws ec2 create-subnet --vpc-id "$VPC_ID" --cidr-block "$SUBNET_CIDR" \
    --availability-zone "${REGION}a" --query Subnet.SubnetId --output text)
  tag "$SUBNET_ID"
  aws ec2 modify-subnet-attribute --subnet-id "$SUBNET_ID" --map-public-ip-on-launch >/dev/null
  echo "==> created subnet $SUBNET_ID"
fi

RTB_ID=$(tagged route-tables 'RouteTables[0].RouteTableId')
if [ -z "$RTB_ID" ]; then
  RTB_ID=$(aws ec2 create-route-table --vpc-id "$VPC_ID" --query RouteTable.RouteTableId --output text)
  tag "$RTB_ID"
  aws ec2 create-route --route-table-id "$RTB_ID" --destination-cidr-block 0.0.0.0/0 \
    --gateway-id "$IGW_ID" >/dev/null
  aws ec2 associate-route-table --route-table-id "$RTB_ID" --subnet-id "$SUBNET_ID" >/dev/null
  echo "==> created route table $RTB_ID"
fi

SG_ID=$(aws ec2 describe-security-groups --filters "Name=group-name,Values=$NAME" \
  "Name=vpc-id,Values=$VPC_ID" --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || true)
[ "$SG_ID" = None ] && SG_ID=
if [ -z "$SG_ID" ]; then
  SG_ID=$(aws ec2 create-security-group --group-name "$NAME" --vpc-id "$VPC_ID" \
    --description "openhouse demo: ssh from operator only, http/s public" \
    --query GroupId --output text)
  tag "$SG_ID"
  # SSH is operator-only; add more with ./add-ssh-ip.sh
  aws ec2 authorize-security-group-ingress --group-id "$SG_ID" --ip-permissions \
    "IpProtocol=tcp,FromPort=22,ToPort=22,IpRanges=[{CidrIp=$MY_IP,Description=operator}]" >/dev/null
  aws ec2 authorize-security-group-ingress --group-id "$SG_ID" --ip-permissions \
    "IpProtocol=tcp,FromPort=80,ToPort=80,IpRanges=[{CidrIp=0.0.0.0/0,Description=public-http}]" \
    "IpProtocol=tcp,FromPort=443,ToPort=443,IpRanges=[{CidrIp=0.0.0.0/0,Description=public-https}]" >/dev/null
  echo "==> created security group $SG_ID"
fi

if ! aws ec2 describe-key-pairs --key-names "$NAME" >/dev/null 2>&1; then
  aws ec2 create-key-pair --key-name "$NAME" --query KeyMaterial --output text > "$PEM"
  chmod 600 "$PEM"
  echo "==> created key pair -> $PEM"
fi
[ -f "$PEM" ] || { echo "key pair '$NAME' exists in AWS but $PEM is missing locally." >&2
                   echo "Delete it (aws ec2 delete-key-pair --key-name $NAME) and re-run." >&2; exit 1; }

AMI=$(aws ec2 describe-images --owners 099720109477 \
  --filters 'Name=name,Values=ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*' \
            'Name=state,Values=available' \
  --query 'reverse(sort_by(Images,&CreationDate))[0].ImageId' --output text)

INSTANCE_ID=$(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=$NAME" "Name=instance-state-name,Values=pending,running" \
  --query 'Reservations[0].Instances[0].InstanceId' --output text 2>/dev/null || true)
[ "$INSTANCE_ID" = None ] && INSTANCE_ID=

if [ -z "$INSTANCE_ID" ]; then
  echo "==> launching $INSTANCE_TYPE ($AMI)"
  INSTANCE_ID=$(aws ec2 run-instances --image-id "$AMI" --instance-type "$INSTANCE_TYPE" \
    --key-name "$NAME" --subnet-id "$SUBNET_ID" --security-group-ids "$SG_ID" \
    --block-device-mappings 'DeviceName=/dev/sda1,Ebs={VolumeSize=60,VolumeType=gp3}' \
    --user-data file://cloud-init.yaml \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$NAME}]" \
    --query 'Instances[0].InstanceId' --output text)
fi

aws ec2 wait instance-running --instance-ids "$INSTANCE_ID"
PUBLIC_IP=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)

# quoted: the repo path contains a space
cat > "$STATE" <<EOF
VPC_ID="$VPC_ID"
IGW_ID="$IGW_ID"
SUBNET_ID="$SUBNET_ID"
RTB_ID="$RTB_ID"
SG_ID="$SG_ID"
INSTANCE_ID="$INSTANCE_ID"
PUBLIC_IP="$PUBLIC_IP"
PEM="$PEM"
EOF

echo
echo "✅ instance $INSTANCE_ID at $PUBLIC_IP"
echo "   ssh -i $PEM ubuntu@$PUBLIC_IP"
echo "   app will serve on http://$PUBLIC_IP/"
echo "   state -> $STATE"
