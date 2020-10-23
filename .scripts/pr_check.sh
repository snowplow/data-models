#!/bin/bash

# expected input:
# bash pr_check {path_to_sql_runner} {database} {major version} {credentials (optional)}

script_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )

echo "pr_check: Starting 10 e2e iterations"

for i in {1..10}; do
  echo "pr_check: Starting e2e run $i";

  bash $script_path/e2e.sh $1 $2 $3 $4;

  echo "pr_check: e2e run $i Done";

done || exit 1

echo "pr_check: Done"
