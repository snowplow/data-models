# Configuring the 02-screen-views playbooks

The screen views module runs the standard mobile screen views model. It takes the `mobile_events_staged` table - produced by the Base module - as an input.

`01-screen-views-main.yml.tmpl` runs the main mobile model logic. `99-screen-views-complete.yml.tmpl` runs the cleanup steps afterwards. `XX-destroy-screen-views.yml.tmpl` destroys all tables and manifests, for a complete rebuild.

## Configuration quick reference

### 01-screen-views-main

`:input_schema:`       name of atomic dataset

`:scratch_schema:`     name of scratch dataset  

`:output_schema:`      name of derived dataset

`:entropy:`            string to append to all tables, to test without affecting prod tables (eg. `_test` produces tables like `screen_views_test`). Must match entropy value used for all other modules in a given run.

`:stage_next:`         update staging tables - set to true if running the next module. If true, make sure that the next module includes a 'complete' step.

`:upsert_lookback_days:`    default 30. Period of time (in days) to look back over the production table in order to find rows to delete when upserting data. Where performance is not a concern, should be set to as long a value as possible.

`:skip_derived:`       default false. Set to true to skip insert to production screen views table.

`:cluster_by:`         array - default `[app_id, device_user_id, session_id]`. Columns used to cluster the `mobile_screen_views_staged` and `mobile_screen_views` tables. Override if your use case requires different clustering. Note clustering is defined during table creation and therefore to recluster pre-existing tables one must either a) drop and recompute the tables or b) copy the data over to new tables with the custom clustering applied.

**Note:** `upsert_lookback_days` can produce duplicates if set to too short a window.

### 99-screen-views-complete

`:scratch_schema:`     name of scratch dataset

`:output_schema:`      name of derived dataset

`:entropy:`            string to append to all tables, to test without affecting prod tables. Must match entropy value used for all other modules in a given run.

`:cleanup_mode:`       options: `debug` - only keeps main tables. `trace` - keeps all tables. `all` - cleans up everything.

`:ends_run:`           set to true if there are no subsequent modules in the run, false otherwise.

### XX-destroy-screen-views

`:scratch_schema:`     name of scratch dataset

`:output_schema:`      name of derived dataset

`:entropy:`            string to append to all tables, to test without affecting prod tables. Must match entropy value used for all other modules in a given run.

`:cleanup_mode:`       should be set to "all" for a destroy.

`:ends_run:`           should be set to true for a destroy.

## Order of execution

Custom steps should run before `99-screen-views-complete.yml.tmpl`, but after `01-screen-views-main.yml.tmpl`, as follows:

1: 01-screen-views-main.yml.tmpl

2: AA-my-custom-screen-views-level-module.yml.tmpl

3: 99-screen-views-complete.yml.tmpl

Custom modules should produce tables which join to the screen views table rather than altering it where possible.
