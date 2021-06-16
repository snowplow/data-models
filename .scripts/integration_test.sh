#!/bin/bash

# Expected input:
# -b (binary) path to sql-runner binary
# -d (database) target database for expectations
# -a (auth) optional credentials for database target
# -m (model) target model to run i.e. web or mobile

while getopts 'b:d:a:m:' opt
do
  case $opt in
    b) SQL_RUNNER_PATH=$OPTARG ;;
    d) DATABASE=$OPTARG ;;
    a) CREDENTIALS=$OPTARG ;;
		m) MODEL=$OPTARG ;;
  esac
done

repo_root_path=$( cd "$(dirname "$(dirname "${BASH_SOURCE[0]}")")" && pwd -P )
script_path="${repo_root_path}/.scripts"
config_dir="${repo_root_path}/$MODEL/v1/$DATABASE/sql-runner/configs"

# Set credentials via env vars
export BIGQUERY_CREDS=${BIGQUERY_CREDS:-$CREDENTIALS}
export REDSHIFT_PASSWORD=${REDSHIFT_PASSWORD:-$CREDENTIALS}
export SNOWFLAKE_PASSWORD=${SNOWFLAKE_PASSWORD:-$CREDENTIALS}

echo "integration_check: Starting 5 runs"

for i in {1..5}; do
	
  echo "integration_check: Starting run $i";

  bash .scripts/run_config.sh -b sql-runner -c $config_dir/pre_test.json -t $script_path/templates/$DATABASE.yml.tmpl -v .test/integration_tests/$MODEL/v1/${DATABASE}_variables.yml.tmpl || exit;

  echo "integration_check: Checking actual vs. expected for the events_staged table";

  bash $script_path/run_test.sh -m $MODEL -d $DATABASE -c events_staged_integration_test_${i} || exit 1;

  bash .scripts/run_config.sh -b sql-runner -c $config_dir/post_test.json -t $script_path/templates/$DATABASE.yml.tmpl -v .test/integration_tests/$MODEL/v1/${DATABASE}_variables.yml.tmpl || exit;

  echo "integration_check: run $i done";

done || exit 1

echo "integration_check: Checking actual vs. expected for derived tables";

bash $script_path/run_test.sh -m $MODEL -d $DATABASE -c perm_integration_test_tables || exit 1;

echo "integration_check: Checking standard tests against derived tables";

bash $script_path/run_test.sh -m $MODEL -d $DATABASE -c perm_tables || exit 1;

echo "integration_check: Done"
