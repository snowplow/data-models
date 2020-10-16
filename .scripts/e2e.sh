#!/bin/bash

# expected input:
# bash e2e {path_to_sql_runner} {database} {major version} {credentials (optional)}

script_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )

echo "e2e: Running all modules";

bash $script_path/run_playbooks.sh $1 $2 $3 'standard/01-base/01-base-main,standard/02-page-views/01-page-views-main,standard/03-sessions/01-sessions-main,standard/04-users/01-users-main' $4;

echo "e2e: Running great expectations";

bash $script_path/run_test.sh $2 $3 temp_tables $4;

echo "e2e: Running completion steps";

bash $script_path/run_playbooks.sh $1 $2 $3 'standard/01-base/99-base-complete,standard/02-page-views/99-page-views-complete,standard/03-sessions/99-sessions-complete,standard/04-users/99-users-complete' $4;

bash $script_path/run_test.sh $2 $3 perm_tables $4;

echo "e2e: Done";
