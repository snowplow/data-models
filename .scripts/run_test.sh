#!/bin/bash

# Expected input:
# -d (database) target database for expectations
# -c (config) expectation config name
# -a (auth) optional credentials for database target

while getopts 'd:c:a:' v
do
  case $v in
    d) DATABASE=$OPTARG ;;
    c) CONFIG=$OPTARG ;;
    a) CREDENTIALS=$OPTARG ;;

  esac
done

# set working dir
root_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )/..
cd $root_path/.test

set -e

if [ "$DATABASE" == "bigquery" ]; then

  BIGQUERY_CREDS=${BIGQUERY_CREDS:-$CREDENTIALS}

  if [ -n "$BIGQUERY_CREDS" ]; then

    # If creds provided via env var or argument, set trap to clean up, then create creds file.
    cleanup() {
      echo "run_test: Removing credentials file"
      rm -f $root_path/tmp/bq_creds.json
    }
    trap cleanup EXIT

    echo "run_test: writing bq creds to file"
    echo $BIGQUERY_CREDS > $root_path/tmp/bq_creds.json

  fi

  # Set GOOGLE_APPLICATION_CREDENTIALS env var.
  export GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS:-$root_path/tmp/bq_creds.json}

  # Set dummy env vars if not set already (to avoid config error)
  export REDSHIFT_PASSWORD=${REDSHIFT_PASSWORD:-'dummy'}
  export SNOWFLAKE_PASSWORD=${SNOWFLAKE_PASSWORD:-'dummy'}

else

  # If not BQ, take the relevant env var if it exists, set it to whatever's provided otherwise.
  export REDSHIFT_PASSWORD=${REDSHIFT_PASSWORD:-$CREDENTIALS:-'dummy'}
  export SNOWFLAKE_PASSWORD=${SNOWFLAKE_PASSWORD:-$CREDENTIALS:-'dummy'}
  export GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS:-'dummy'}

fi

echo "run_test: running v1 expectations for $DATABASE"
great_expectations validation-operator run --validation_config_file great_expectations/validation_configs/web/v1/$DATABASE/$CONFIG.json --run_name "${DATABASE}_v1_${CONFIG}"
