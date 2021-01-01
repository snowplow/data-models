# Configuring the 02-page-views playbooks

The page views module runs the standard web page views model. It takes as input the `events_staged` table, produced by the Base module.

 - `01-page-views-main.yml.tmpl` runs the main web model logic.
 - `99-page-views-complete.yml.tmpl` truncates the input table, and runs cleanup steps afterwards.
 - `XX-destroy-page-views.yml.tmpl` destroys all tables and manifests, for a complete rebuild.

## Configuration quick reference

### 01-page-views-main

`:scratch_schema:`     Name of scratch schema

`:output_schema:`      Name of derived schema

`:entropy:`            String to append to all tables, to test without affecting prod tables (eg. `_test` produces tables like `page_views_test`). Must match entropy value used for all other modules in a given run.

`:stage_next:`         Update staging tables - set to true if running the next module. If true, make sure that the next module includes a 'complete' step.

`:upsert_lookback_days:`    Default 30. Period of time (in days) to look back over the production table in order to find rows to delete when upserting data. Where performance is not a concern, should be set to as long a value as possible.

`:skip_derived:`       Default false. Set to true to skip insert to production page views table.

`:minimumVisitLength:` The value of minimumVisitLength configured in the Javascript tracker.

`:heartbeat:`          The value of heartbeat configured in the Javascript tracker.

`:ua_bot_filter:`      Configuration to filter out bots via useragent string pattern match.

`:iab:`                Configuration to include data from the IAB enrichment.

`:ua_parser:`          Configuration to include data from the UA Parser enrichment.

`:yauaa:`              Configuration to include data from the YAUAA enrichment.


**Note:** `upsert_lookback_days` can produce duplicates if set to too short a window.

### 99-page-views-complete

`:scratch_schema:`     Name of scratch schema

`:output_schema:`      Name of derived schema

`:entropy:`            String to append to all tables, to test without affecting prod tables. Must match entropy value used for all other modules in a given run.

`:cleanup_mode:`       Options: `debug` - only keeps main tables. `trace` - keeps all tables. `all` - cleans up everything.

`:ends_run:`           Set to true if there are no subsequent modules in the run, false otherwise.

### XX-destroy-page-views

`:scratch_schema:`     Name of scratch schema

`:output_schema:`      Name of derived schema

`:entropy:`            String to append to all tables, to test without affecting prod tables. Must match entropy value used for all other modules in a given run.

`:cleanup_mode:`       Should be set to "all" for a destroy.

`:ends_run:`           Should be set to true for a destroy.


## Order of execution

Custom steps should run before `99-page-views-complete.yml.tmpl`, but after `01-page-views-main.yml.tmpl`, as follows:

1. `01-page-views-main.yml.tmpl`
2. `AA-my-custom-page-views-level-module.yml.tmpl`
3. `99-page-views-complete.yml.tmpl`

Custom modules should produce tables which join to the page views table rather than altering it where possible.
