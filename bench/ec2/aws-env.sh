#!/usr/bin/env bash
# Source this before running aws commands:  source ../aws-env.sh
#
# Credential resolution, in order:
#  - ~/.aws/credentials in shell-export form (export AWS_ACCESS_KEY_ID=...):
#    source it as env vars and point CLI file paths at /dev/null so the CLI
#    uses env vars instead of trying (and failing) to parse it as INI
#  - otherwise normal AWS resolution via AWS_PROFILE (eg SSO:
#    aws sso login --profile=<p> && export AWS_PROFILE=<p>)
if [ -f ~/.aws/credentials ]; then
  set -a
  # shellcheck disable=SC1090
  source ~/.aws/credentials
  set +a
  unset AWS_PROFILE
  export AWS_SHARED_CREDENTIALS_FILE=/dev/null
  export AWS_CONFIG_FILE=/dev/null
  # config file is disabled above, so a region must come from env
  export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-ap-south-1}"
else
  : "${AWS_PROFILE:?no ~/.aws/credentials; export AWS_PROFILE (after aws sso login --profile=<p>)}"
fi
