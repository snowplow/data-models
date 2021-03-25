#!/bin/bash

# Expected input:
# -b (binary) path to sql-runner binary
# -d (database) target database for expectations
# -a (auth) optional credentials for database target

while getopts 'b:d:a:m:' v
do
  case $v in
    b) SQL_RUNNER_PATH=$OPTARG ;;
    d) DATABASE=$OPTARG ;;
    a) CREDENTIALS=$OPTARG ;;
    m) MODEL=$OPTARG ;;
  esac
done

# Set credentials via env vars
export BIGQUERY_CREDS=${BIGQUERY_CREDS:-$CREDENTIALS}
export REDSHIFT_PASSWORD=${REDSHIFT_PASSWORD:-$CREDENTIALS}
export SNOWFLAKE_PASSWORD=${SNOWFLAKE_PASSWORD:-$CREDENTIALS}


repo_root_path=$( cd "$(dirname "$(dirname "${BASH_SOURCE[0]}")")" && pwd -P )
script_path="${repo_root_path}/.scripts"
config_dir="${repo_root_path}/$MODEL/v1/$DATABASE/sql-runner/configs"

echo "e2e: Running all modules";

bash $script_path/run_config.sh -c $config_dir/pre_test.json -b $SQL_RUNNER_PATH -t $script_path/templates/$DATABASE.yml.tmpl || exit 1;

echo "e2e: Running great expectations";

bash $script_path/run_test.sh -m $MODEL -d $DATABASE -c temp_tables || exit 1;

echo "e2e: Running completion steps";

bash $script_path/run_config.sh -c $config_dir/post_test.json -b $SQL_RUNNER_PATH -t $script_path/templates/$DATABASE.yml.tmpl || exit 1;

bash $script_path/run_test.sh -m $MODEL -d $DATABASE -c perm_tables || exit 1;

echo "e2e: Done";
