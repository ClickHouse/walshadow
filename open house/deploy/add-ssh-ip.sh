#!/usr/bin/env bash
# Allow SSH from another address.   ./add-ssh-ip.sh 203.0.113.9 "kaushik laptop"
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"; . ./state.env
export AWS_SHARED_CREDENTIALS_FILE=/dev/null AWS_CONFIG_FILE=/dev/null
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
IP="${1:?usage: add-ssh-ip.sh <ip-or-cidr> [description]}"
case "$IP" in */*) ;; *) IP="$IP/32" ;; esac
aws ec2 authorize-security-group-ingress --group-id "$SG_ID" --ip-permissions \
  "IpProtocol=tcp,FromPort=22,ToPort=22,IpRanges=[{CidrIp=$IP,Description=${2:-added}}]" >/dev/null \
  && echo "✅ SSH allowed from $IP"
aws ec2 describe-security-groups --group-ids "$SG_ID" \
  --query 'SecurityGroups[0].IpPermissions[?FromPort==`22`].IpRanges[]' --output table
