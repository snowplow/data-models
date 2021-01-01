# Configuring the 03-sessions playbooks

The sessions module runs the standard web sessions model - it takes as input the `page_views_staged` table, produced by the Page Views module.

 - `01-sessions-main.yml.tmpl` runs the main web model logic.
 - `99-sessions-complete.yml.tmpl` truncates the input table, and runs cleanup steps afterwards.
 - `XX-destroy-sessions.yml.tmpl` destroys all tables and manifests, for a complete rebuild.

## Configuration quick reference

### 01-sessions-main

`:scratch_schema:`     Name of scratch schema

`:output_schema:`      Name of derived schema

`:entropy:`            String to append to all tables, to test without affecting prod tables (eg. `_test` produces tables like `sessions_test`). Must match entropy value used for all other modules in a given run.

`:stage_next:`         Update staging tables - set to true if running the next module. If true, make sure that the next module includes a 'complete' step.

`:upsert_lookback_days:`    Default 30. Period of time (in days) to look back over the production table in order to find rows to delete when upserting data. Where performance is not a concern, should be set to as long a value as possible.

`:skip_derived:`       Default false. Set to true to skip insert to production sessions table.

**Note:** `upsert_lookback_days` can produce duplicates if set to too short a window.

### 99-sessions-complete

`:scratch_schema:`     Name of scratch schema

`:output_schema:`      Name of derived schema

`:entropy:`            String to append to all tables, to test without affecting prod tables. Must match entropy value used for all other modules in a given run.

`:cleanup_mode:`       Options: `debug` - only keeps main tables. `trace` - keeps all tables. `all` - cleans up everything.

`:ends_run:`           Set to true if there are no subsequent modules in the run, false otherwise.


### XX-destroy-sessions

`:scratch_schema:`     Name of scratch schema

`:output_schema:`      Name of derived schema

`:entropy:`            String to append to all tables, to test without affecting prod tables. Must match entropy value used for all other modules in a given run.

`:cleanup_mode:`       Should be set to `all` for a destroy.

`:ends_run:`           Should be set to true for a destroy.


## Order of execution

Custom steps should run before `99-sessions-complete.yml.tmpl`, but after `01-sessions-main.yml.tmpl`, as follows:

1. `01-sessions-main.yml.tmpl`
2. `AA-my-custom-sessions-level-module.yml.tmpl`
3. `99-sessions-complete.yml.tmpl`

Custom modules should produce tables which join to the sessions table rather than altering it where possible.
