#!/bin/bash

# Expected input:
# -b (binary) path to sql-runner binary
# -c (config) path to config
# -a (auth) optional credentials for database target
# -p (print SQL) use sql-runner fillTemplates to print pure sql
# -d (dryRun) use sql-runner dry run
# -o (output path) path to store output of sql-runner to sql file (to be used in conjunction with p)
# -t (target template) path to target template to use (minimizes risk of credential leak)
# -v (varialbles template) path to variables template to use

while getopts 'pdb:c:a:o:t:v:' opt
do
  case $opt in
    b) SQL_RUNNER_PATH=$OPTARG ;;
    c) CONFIG_PATH=$OPTARG ;;
    a) CREDENTIALS=$OPTARG ;;
    p) FILL_TEMPLATES='-fillTemplates' ;;
    d) DRY_RUN='-dryRun' ;;
    o) OUTPUT_PATH=$OPTARG ;;
    t) TARGET_TEMPLATE=$OPTARG ;;
    v) VARIABLES_TEMPLATE=$OPTARG
  esac
done

root_path=$( cd "$(dirname "$(dirname "${BASH_SOURCE[0]}")")" && pwd -P )

# Use jq to grab playbooks into a bash array in order of appearance, and database target into a var.
playbooks=($(cat $CONFIG_PATH | jq -r '.data.playbooks[].playbook | @sh' | tr -d \'))
target=$(cat $CONFIG_PATH | jq -r '.data.storage')
model_path=$( cd "$(dirname "$(dirname "${CONFIG_PATH}")")" && pwd -P )

set -e

cleanup() {
  echo "run_config: Removing playbook file"
  rm -f $root_path/tmp/current_playbook.yml
}
trap cleanup EXIT

mkdir -p "${root_path}/tmp"

if [ $target == "Default" ]; then

  storage_target=$(basename $(dirname "${model_path}"))
  if [ "${storage_target}" = "redshift" ] ; then
    PASSWORD=${REDSHIFT_PASSWORD:-$CREDENTIALS}
  elif [ "${storage_target}" = "snowflake" ] ; then
    PASSWORD=${SNOWFLAKE_PASSWORD:-$CREDENTIALS}
  else
    echo "Storage target mismatch!" >&2 ; exit 1;
  fi

elif [ "$target" == "BigQuery" ]; then

  export GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS:-$CREDENTIALS}

fi

for i in "${playbooks[@]}";
do

  if [ ! -z "$TARGET_TEMPLATE" ]; then

    # Append template and playbook, subbing in credentials:
    sed "s/PASSWORD_PLACEHOLDER/$PASSWORD/" $TARGET_TEMPLATE > $root_path/tmp/current_playbook.yml
    sed "1,/^:variables:$/d" $model_path/playbooks/$i.yml.tmpl >> $root_path/tmp/current_playbook.yml

  else

    # sub in credentials only if target template not provided
    sed "s/PASSWORD_PLACEHOLDER/$PASSWORD/" $model_path/playbooks/$i.yml.tmpl > $root_path/tmp/current_playbook.yml

  fi

  if [ ! -z "$VARIABLES_TEMPLATE" ]; then

    # Sub in any variables if specified
    awk -F':' 'NR==FNR{a[$2]=$0;next} /:variables:/{flag=1} /:steps:/{flag=0} a[$2]&&flag{$0=a[$2]}1' $root_path/$VARIABLES_TEMPLATE $root_path/tmp/current_playbook.yml > $root_path/tmp/current_playbook.tmp && mv $root_path/tmp/current_playbook.tmp $root_path/tmp/current_playbook.yml

  fi

  # If printing sql to file, mkdirs and set path vars
  if [ ! -z "$OUTPUT_PATH" ]; then
    mkdir -p $OUTPUT_PATH
    OUTPUT_FILE="${OUTPUT_PATH%/}/$(basename "${i}").sql"
    TO_OUTPUT=" &> $OUTPUT_FILE"
  fi

  echo "run_config: starting playbook: $i"

  if [ ! -z "$FILL_TEMPLATES" ]; then
    set +e
  fi

  # Create run command
  run_command="(eval '$SQL_RUNNER_PATH -playbook $root_path/tmp/current_playbook.yml -sqlroot $model_path/sql $FILL_TEMPLATES $DRY_RUN') $TO_OUTPUT"

  eval $run_command

  # If printing sql to file, comment out metadata strings in the SQL
  if [ ! -z "$OUTPUT_PATH" ]; then
    mkdir -p $OUTPUT_PATH
    perl -i -pe 's/^([0-9]{4}\/(0[1-9]|1[0-2])\/(0[1-9]|[1-2][0-9]|3[0-1]) (2[0-3]|[01][0-9]):[0-5][0-9]:[0-5][0-9] (Step name: |WARNING: )|Query name: |Query path: )/-- \1/' "$OUTPUT_FILE"
  fi

  set -e

  echo "run_config: done with playbook: $i";

  echo $OUTPUT_PATH
done
