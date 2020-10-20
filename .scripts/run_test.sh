#!/bin/bash

# Expected input:
# bash run_test.sh {database} {major version} {credentials (optional)}

# set working dir
root_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )/..
cd $root_path/.test

if [ "$1" == "bigquery" ]; then

  BIGQUERY_CREDS=${BIGQUERY_CREDS:-$3}

  if [ -n "$BIGQUERY_CREDS" ]; then

    # If creds provided via env var or argument, set trap to clean up, then create creds file.
    set -e
    cleanup() {
      echo "Removing credentials file"
      rm -f $root_path/tmp/bq_creds.json
    }
    trap cleanup EXIT

    echo "writing bq creds to file"
    echo $BIGQUERY_CREDS > $root_path/tmp/bq_creds.json

  fi

  # Set GOOGLE_APPLICATION_CREDENTIALS env var.
  export GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS:-$root_path/tmp/bq_creds.json}

  # Set dummy env vars if not set already (to avoid config error)
  export REDSHIFT_PASSWORD=${REDSHIFT_PASSWORD:-'dummy'}
  export SNOWFLAKE_PASSWORD=${SNOWFLAKE_PASSWORD:-'dummy'}

else

  # If not BQ, take the relevant env var if it exists, set it to whatever's provided otherwise.
  export REDSHIFT_PASSWORD=${REDSHIFT_PASSWORD:-$3:-'dummy'}
  export SNOWFLAKE_PASSWORD=${SNOWFLAKE_PASSWORD:-$3:-'dummy'}
  export GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS:-'dummy'}

fi

echo "running $2 expectations for $1"
great_expectations validation-operator run --validation_config_file great_expectations/validation_configs/web/$2/$1.json --run_name $1_$2
