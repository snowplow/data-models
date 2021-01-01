# Configuring the 04-users playbooks

The users module runs the standard web users model - it takes as input the `sessions_userid_manifest_staged` table, produced by the Sessions module.

 - `01-users-main.yml.tmpl` runs the main web model logic.
 - `99-users-complete.yml.tmpl` truncates the input table, and runs cleanup steps afterwards.
 - `XX-destroy-users.yml.tmpl` destroys all tables and manifests, for a complete rebuild.

## Configuration quick reference

### 01-users-main

`:scratch_schema:`     Name of scratch schema

`:output_schema:`      Name of derived schema

`:entropy:`            String to append to all tables, to test without affecting prod tables (eg. `_test` produces tables like `users_test`). Must match entropy value used for all other modules in a given run.

`:start_date:`         Start date, used to seed manifest.

`:skip_derived:`       Default false. Set to true to skip insert to production users table.


### 99-users-complete

`:scratch_schema:`     Name of scratch schema

`:output_schema:`      Name of derived schema

`:entropy:`            String to append to all tables, to test without affecting prod tables. Must match entropy value used for all other modules in a given run.

`:cleanup_mode:`       Options: `debug` - only keeps main tables. `trace` - keeps all tables. `all` - cleans up everything.

`:ends_run:`           set to true if there are no subsequent modules in the run, false otherwise.


# XX-destroy-users

`:scratch_schema:`     Name of scratch schema

`:output_schema:`      Name of derived schema

`:entropy:`            String to append to all tables, to test without affecting prod tables. Must match entropy value used for all other modules in a given run.

`:cleanup_mode:`       Should be set to `all` for a destroy.

`:ends_run:`           Should be set to true for a destroy.


## Order of execution

Custom steps should run before `99-users-complete.yml.tmpl`, but after `01-users-main.yml.tmpl`, as follows:

1. `01-users-main.yml.tmpl`
2. `AA-my-custom-users-level-module.yml.tmpl`
3. `99-users-complete.yml.tmpl`

Custom modules should produce tables which join to the users table rather than altering it where possible.
