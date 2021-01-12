# Configuring the 02-page-views playbooks

The page views module runs the standard web page views model. It takes the `events_staged` table - produced by the Base module - as an input (and also queries atomic data).

`01-page-views-main.yml.tmpl` runs the main web model logic. `99-page-views-complete.yml.tmpl` truncates the input table, and runs cleanup steps afterwards. `XX-destroy-page-views.yml.tmpl` destroys all tables and manifests, for a complete rebuild.

## Configuration quick reference

### 01-page-views-main

`:scratch_schema:`     name of scratch dataset  

`:output_schema:`      name of derived dataset

`:entropy:`            string to append to all tables, to test without affecting prod tables (eg. `_test` produces tables like `page_views_test`). Must match entropy value used for all other modules in a given run.

`:minimumVisitLength:` The value of minimumVisitLength configured in the Javascript tracker.

`:heartbeat:`          The value of heartbeat configured in the Javascript tracker.

`:ua_bot_filter:`      Boolean - Configure whether to filter out bots via useragent string pattern match.

`:iab:`                Boolean -  Configure whether to include data from the IAB enrichment.

`:ua_parser:`          Boolean -  Configure whether to include data from the UA Parser enrichment.

`:yauaa:`              Boolean -  Configure whether to include data from the YAUAA enrichment.

`:upsert_lookback:`    Default 30. Period of time (in days) to look back over the target table in order to find rows to delete when committing data to a table. Where performance is not a concern, should be set to as long a value as possible.

`:skip_derived:`       Default false. Set to true to skip insert to production page views table.

`:stage_next:`         Update staging tables - set to true if running the next module. If true, make sure that the next module includes a 'complete' step.

**Note:** `upsert_lookback` can produce duplicates if set to too short a window.

### 99-page-views-complete

`:scratch_schema:`     name of scratch dataset

`:output_schema:`      name of derived dataset

`:entropy:`            string to append to all tables, to test without affecting prod tables. Must match entropy value used for all other modules in a given run.

`:cleanup_mode:`       Options: `debug` - only keeps main tables.

`trace` - keeps all tables. `all` - cleans up everything.

`:ends_run:`           set to true if there are no subsequent modules in the run, false otherwise.

### XX-destroy-page-views

`:scratch_schema:`     name of scratch dataset

`:output_schema:`      name of derived dataset

`:entropy:`            string to append to all tables, to test without affecting prod tables. Must match entropy value used for all other modules in a given run.

`:cleanup_mode:`       Should be set to "all" for a destroy.

`:ends_run:`           Should be set to true for a destroy.

## Order of execution

Custom steps should run before `99-page-views-complete.yml.tmpl`, but after `01-page-views-main.yml.tmpl`, as follows:

1: 01-page-views-main.yml.tmpl
2: AA-my-custom-page-views-level-module.yml.tmpl
3: 99-page-views-complete.yml.tmpl

Custom modules should produce tables which join to the page views table rather than altering it where possible.
