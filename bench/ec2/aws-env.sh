#!/usr/bin/env bash
# Source this before running aws commands:  source ../aws-env.sh
#
# ~/.aws/credentials here is in shell-export form (export AWS_ACCESS_KEY_ID=...),
# which the AWS CLI cannot parse as an INI credentials file. So we source it as
# env vars and point the CLI's file paths at /dev/null so it uses the env vars
# instead of trying (and failing) to parse those files.
set -a
# shellcheck disable=SC1090
source ~/.aws/credentials
set +a
unset AWS_PROFILE
export AWS_SHARED_CREDENTIALS_FILE=/dev/null
export AWS_CONFIG_FILE=/dev/null
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-ap-south-1}"
