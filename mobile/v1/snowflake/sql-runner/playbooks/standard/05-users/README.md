# Configuring the 05-users playbooks

The users module runs the standard mobile sessions model - it takes the `mobile_sessions_userid_manifest_staged` table - produced by the Sessions module - as an input.

`01-users-main.yml.tmpl` runs the main mobile model logic. `99-users-complete.yml.tmpl` truncates the input table, and runs cleanup steps afterwards. `XX-destroy-users.yml.tmpl` destroys all tables and manifests, for a complete rebuild.

## Configuration quick reference

### 01-users-main

`:scratch_schema:`     name of scratch schema  

`:output_schema:`      name of derived schema

`:entropy:`            string to append to all tables, to test without affecting prod tables (eg. `_test` produces tables like `mobile_users_test`). Must match entropy value used for all other modules in a given run.

`:skip_derived:`       default false. Set to true to skip insert to production users table.

**Note:** `upsert_lookback_days` can produce duplicates if set to too short a window.

### 99-users-complete

`:scratch_schema:`     name of scratch schema

`:output_schema:`      name of derived schema

`:entropy:`            string to append to all tables, to test without affecting prod tables. Must match entropy value used for all other modules in a given run.

`:cleanup_mode:`       options: `debug` - only keeps main tables. `trace` - keeps all tables. `all` - cleans up everything.

`:ends_run:`           set to true if there are no subsequent modules in the run, false otherwise.

### XX-destroy-users

`:scratch_schema:`     name of scratch schema

`:output_schema:`      name of derived schema

`:entropy:`            string to append to all tables, to test without affecting prod tables. Must match entropy value used for all other modules in a given run.

`:cleanup_mode:`       should be set to `all` for a destroy.

`:ends_run:`           should be set to true for a destroy.

## Order of execution

Custom steps should run before `99-users-complete.yml.tmpl`, but after `01-users-main.yml.tmpl`, as follows:

1: 01-users-main.yml.tmpl

2: AA-my-custom-users-level-module.yml.tmpl

3: 99-users-complete.yml.tmpl

Custom modules should produce tables which join to the users table rather than altering it where possible.
