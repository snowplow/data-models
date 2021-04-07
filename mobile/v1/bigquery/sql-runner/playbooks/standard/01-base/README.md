# Configuring the 01-base playbooks

The Base module applies incremental logic to the atomic data, and produces deduplicated tables for subsequent modules to consume. This module is shared between the web and mobile models, with the `:model:` variable donating which model to run.

`01-base-main.yml.tmpl` runs the main incremental logic. `99-base-complete.yml.tmpl` commits to the manifest, and runs cleanup steps afterwards. `XX-destroy-base.yml.tmpl` destroys all tables and manifests, for a complete rebuild.

## Configuration quick reference

### 01-base-main

`:model:`       			 name of model to run, web or mobile.

`:input_schema:`       name of atomic dataset

`:scratch_schema:`     name of scratch dataset  

`:output_schema:`      name of derived dataset

`:entropy:`            string to append to all tables, to test without affecting prod tables (eg. `_test` produces tables like `mobile_events_staged_test`). Must match entropy value used for all other modules in a given run. Populate with an empty string if no entropy value is needed.

`:stage_next:`         update staging tables - set to true if running the next module. If true, make sure that the next module includes a 'complete' step.

`:start_date:`         start date, used to seed manifest.

`:lookback_window_hours:`    defaults to 6. Period of time (in hours) to look before the latest event in manifest - to account for late arriving data, which comes out of order.

`:days_late_allowed:`  defaults to 3.  Period of time (in days) for which we should include late data. If the difference between collector tstamps for the session start and new event is greater than this value, data for that session will not be processed.

`:update_cadence_days:`     defaults to 7. Period of time (in days) in the future (from the latest event in manifest) to look for new events.

`:session_lookback_days:`   defaults to 365. Period of time (in days) to limit scan on session manifest. Exists to improve performance of model when we have a lot of sessions. Should be set to as large a number as practical.

`:mobile_context:`      boolean - Mobile only. Configure whether to include data from the mobile context.

`:geolocation_context:`    boolean - Mobile only. Configure whether to include data from the geolocation context.

`:application_context:`    boolean - Mobile only. Configure whether to include data from the application context.

`:screen_context:`    boolean - Mobile only. Configure whether to include data from the application context.

`:platform_filters:`		array - Defaults to `web` and `mob` for the web and mobile models respectively. List of platforms to filter events by.

`:app_id_filters:`		array - Optional. List of `app_id` to filter events by.

**Notes:**

`days_late_allowed` can be extended in order to account for incidents which cause very late data - for example downtime on the front end.

`session_lookback_days` can cause incorrect data or duplicates if misconfigured - if events arrive with existing session_ids for sessions which pre-date the `session_lookback_days`, this will cause an issue. However this is very unlikely as the lookback should be far greater than what can be reasonably expected for this behaviour from non-bot activity.

### 99-base-complete

`:scratch_schema:`     name of scratch dataset

`:output_schema:`      name of derived dataset

`:entropy:`            string to append to all tables, to test without affecting prod tables (eg. `_test` produces tables like `web_events_staged_test`). Must match entropy value used for all other modules in a given run.

`:cleanup_mode:`       options: `debug` - only keeps main tables. `trace` - keeps all tables. `all` - cleans up everything.

`:ends_run:`           set to true if there are no subsequent modules in the run, false otherwise.

### XX-destroy-base

`:scratch_schema:`     name of scratch dataset

`:output_schema:`      name of derived dataset

`:entropy:`            string to append to all tables, to test without affecting prod tables (eg. `_test` produces tables like `web_events_staged_test`). Must match entropy value used for all other modules in a given run.

`:cleanup_mode:`       should be set to `all` for a destroy.

`:ends_run:`           should be set to true for a destroy.

## Order of execution

Custom steps should run before `99-base-complete.yml.tmpl`, but after `01-base-main.yml.tmpl`, as follows:

1: 01-base-main.yml.tmpl

2: AA-my-custom-base-level-module.yml.tmpl

3: 99-base-complete.yml.tmpl

Note that one should take care if adding custom logic at this stage, since everything downstream depends on it. For example, if duplicates are introduced here, every downstream join is liable to both suffer performance issues, and increase the number of duplicates exponentially.
