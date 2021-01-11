# Configuring the 03-users playbooks

The users module runs the standard web sessions model - it takes the `sessions_userid_manifest_staged` table - produced by the Sessions module - as an input.

`01-users-main.yml.tmpl` runs the main web model logic. `99-users-complete.yml.tmpl` truncates the input table, and runs cleanup steps afterwards. `XX-destroy-users.yml.tmpl` destroys all tables and manifests, for a complete rebuild.

## Configuration quick reference

### 01-users-main

`:scratch_schema:`     name of scratch dataset  

`:output_schema:`      name of derived dataset

`:entropy:`            string to append to all tables, to test without affecting prod tables (eg. `_test` produces tables like `users_test`). Must match entropy value used for all other modules in a given run.

`:skip_derived:`       Default false. Set to true to skip insert to production users table.

**Note:** upsert_lookback can produce duplicates if set to too short a window.

### 99-users-complete

`:scratch_schema:`     name of scratch dataset

`:output_schema:`      name of derived dataset

`:entropy:`            string to append to all tables, to test without affecting prod tables. Must match entropy value used for all other modules in a given run.

`:cleanup_mode:`       Options: `debug` - only keeps main tables. `trace` - keeps all tables. `all` - cleans up everything.

`:ends_run:`           set to true if there are no subsequent modules in the run, false otherwise.

### XX-destroy-users

`:scratch_schema:`     name of scratch dataset

`:output_schema:`      name of derived dataset

`:entropy:`            string to append to all tables, to test without affecting prod tables. Must match entropy value used for all other modules in a given run.

`:cleanup_mode:`       Should be set to `all` for a destroy.

`:ends_run:`           Should be set to true for a destroy.

## Order of execution

Custom steps should run before `99-users-complete.yml.tmpl`, but after `01-users-main.yml.tmpl`, as follows:

1: 01-users-main.yml.tmpl
2: AA-my-custom-users-level-module.yml.tmpl
3: 99-users-complete.yml.tmpl

Custom modules should produce tables which join to the users table rather than altering it where possible.
