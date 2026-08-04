#!/usr/bin/env bash
# Load before running AWS commands: source ../aws-env.sh
#
# Refuse to run against unexpected account
# Set account and profile in ignored aws.local.env
#
# Example:
#   BENCH_AWS_ACCOUNT=<account-id>
#   BENCH_AWS_PROFILE=<p>
# Run aws sso login --profile=<p> when session expires
#
# Credential resolution, in order:
#  - AWS_PROFILE, then BENCH_AWS_PROFILE
#  - ~/.aws/credentials in shell-export form (export AWS_ACCESS_KEY_ID=...):
#    load environment variables and hide non-INI file from CLI
_bench_ec2_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ ! -f "$_bench_ec2_dir/aws.local.env" ]; then
  echo "missing $_bench_ec2_dir/aws.local.env" >&2
  echo "  set BENCH_AWS_ACCOUNT and BENCH_AWS_PROFILE in that file" >&2
  unset _bench_ec2_dir
  return 1
fi
# shellcheck disable=SC1091
source "$_bench_ec2_dir/aws.local.env"
unset _bench_ec2_dir

: "${BENCH_AWS_ACCOUNT:?set BENCH_AWS_ACCOUNT in bench/ec2/aws.local.env}"
export BENCH_AWS_ACCOUNT
# Pass expected account to Terraform
export TF_VAR_account_id="$BENCH_AWS_ACCOUNT"

# Detect INI-formatted credentials
bench_aws_ini_credentials() { grep -qE '^[[:space:]]*\[' ~/.aws/credentials 2>/dev/null; }

: "${AWS_PROFILE:=${BENCH_AWS_PROFILE:-}}"
if [ -n "$AWS_PROFILE" ]; then
  export AWS_PROFILE
  bench_aws_ini_credentials || export AWS_SHARED_CREDENTIALS_FILE=/dev/null
elif [ -f ~/.aws/credentials ] && ! bench_aws_ini_credentials; then
  set -a
  # shellcheck disable=SC1090
  source ~/.aws/credentials
  set +a
  export AWS_SHARED_CREDENTIALS_FILE=/dev/null
  export AWS_CONFIG_FILE=/dev/null
else
  echo "no profile for AWS account $BENCH_AWS_ACCOUNT" >&2
  echo "  echo BENCH_AWS_PROFILE=<p> >> bench/ec2/aws.local.env && aws sso login --profile=<p>" >&2
  return 1
fi
# Keep region available when config file is disabled
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-ap-south-1}"

# Stop before changing an unexpected account
bench_aws_account_check() {
  local got hint="${AWS_PROFILE:+ --profile=$AWS_PROFILE}"
  got="$(aws sts get-caller-identity --query Account --output text 2>&1)" || {
    echo "aws sts get-caller-identity failed: $got" >&2
    echo "  expired SSO session? aws sso login$hint" >&2
    return 1
  }
  [ "$got" = "$BENCH_AWS_ACCOUNT" ] || {
    echo "wrong AWS account: creds${AWS_PROFILE:+ (profile $AWS_PROFILE)} are in $got, bench infra is in $BENCH_AWS_ACCOUNT" >&2
    echo "  set BENCH_AWS_PROFILE in bench/ec2/aws.local.env to a profile for $BENCH_AWS_ACCOUNT," >&2
    echo "  or set BENCH_AWS_ACCOUNT to work in $got" >&2
    return 1
  }
  export AWS_ACCOUNT_ID="$got"
}
bench_aws_account_check

# Pass checked credentials to Terraform
bench_aws_export_creds() {
  local env_out
  env_out="$(aws configure export-credentials --format env-no-export 2>/dev/null)" || return 0
  set -a
  eval "$env_out"
  set +a
  unset AWS_PROFILE
}
bench_aws_export_creds
