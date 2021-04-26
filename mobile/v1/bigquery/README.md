# BigQuery v1 mobile model README

This readme contains a quickstart guide, and details of how the modules interact with each other. For a guide to configuring each module, there is a README in each of the modules' `playbooks` directory.

To customise the model, we recommend following the guidance found in the README in the `sql/custom` directory.

## Quickstart

### Prerequisites

[SQL-runner](https://github.com/snowplow/sql-runner) must be installed, and a dataset of mobile events from either the Snowplow [iOS tracker](https://docs.snowplowanalytics.com/docs/collecting-data/collecting-from-own-applications/objective-c-tracker/) or [Android tracker](https://docs.snowplowanalytics.com/docs/collecting-data/collecting-from-own-applications/android-tracker/) must be available in the database. The session context and screen view events most both be enabled for the mobile model to run. 

### Configuration

#### Authentication

First, fill in the connection details for the target database in the relevant template in `.scripts/template/bigquery.yml.tmpl`.

Set an environment variable, `GOOGLE_APPLICATION_CREDENTIALS`, to the path of your GBQ json credential file. See the README in `.scripts` for more detail.

#### Contexts

The following contexts can be enabled depending on your tracker configuration:

- Mobile context
- Geolocation context
- Application context
- Screen context

By default they are disabled. For more details on how to enable please see the [README](sql-runner/playbooks/standard/01-base/README.md) in the Base module's playbooks folder.

#### Optional Modules

Currently the app errors module for crash reporting is the only optional module. More will be added in the future as the tracker's functionality expands.

Assuming your tracker is capturing `application_error` events, the module can be enabled within the app errors playbook. For more details on how to enable please see the [README](sql-runner/playbooks/standard/03-optional-modules/01-app-errors/README.md) in the app errors module's playbooks folder.

#### Variables

Variables in each module's playbook can also optionally be configured also. See each playbook directory's README for more detail on configuration of each module.

### Run using the `run_config.sh` script

To run the entire standard model, end to end:

```bash
bash .scripts/run_config.sh -b ~/pathTo/sql-runner -c mobile/v1/bigquery/sql-runner/configs/datamodeling.json -t .scripts/templates/bigquery.yml.tmpl;
```

See the README in the `.scripts/` directory for more details.

## Custom Modules

A guide to creating custom modules can be found in the README of the `sql/custom/` directory of the relevant model. Each custom module created must consist of a set of sql files and a playbook, or set of playbooks. The helper scripts described above can also be used to run custom modules.

## Testing

### Setup

Python3 is required.

Install Great Expectations and dependencies, and configure a datasource:

```bash
cd .test
pip3 install -r requirements.txt
great_expectations datasource new
```

Follow the CLI guide to configure access to your database. The configuration for your datasource will be generated in `.test/great_expectations/config/config_variables.tml` - these values can be replaced by environment variables if desired.

Please be aware that the names of the tables to test have been hardcoded in the [validation configs](.test/great_expectations/validation_configs). If you are using a custom values for any of the `entropy`, `scratch_schema` or `output_schema` variables within your playbooks, you will need to manually ammend the validation configs accordingly.

If you have enabled any optional modules within the main mobile model, you will need to enable tests on these modules too. For more details on how to enable please see the [README](sql-runner/playbooks/tests/00-staging-reconciliation/README.md) in the staging reconciliation module's playbooks folder.

### Using the helper scripts

To run the test suites alone:

```bash
bash .scripts/run_test.sh -d bigquery -c perm_tables -m mobile -a {credentials (optional)}
bash .scripts/run_test.sh -d bigquery -c temp_tables -m mobile -a {credentials (optional)}
```

To run an entire run of the standard model, and tests end to end:

```bash
bash .scripts/e2e.sh -b {path_to_sql_runner} -d bigquery -m mobile -a {credentials (optional)}
```

To run a full battery of ten runs of the standard model, and tests:

```bash
bash .scripts/pr_check.sh -b {path_to_sql_runner} -d bigquery -m mobile -a {credentials (optional)}
```

### Adding to tests

Check out the [Great Expectations documentation](https://docs.greatexpectations.io/en/latest/) for guidance on using it to run existing test suites directly, create new expectations, use the profiler, and autogenerate data documentation.

Quickstart to create a new test suite:

`great_expectations suite new`

## Modules detail

### 01-base

Inputs:             atomic tables, `{{.output_schema}}.mobile_base_event_id_manifest`, `{{.output_schema}}.mobile_base_session_id_manifest`

Persistent Outputs: `{{.scratch_schema}}.mobile_events_staged`,

Temporary Outputs:  `{{.scratch_schema}}.mobile_events_this_run`, `{{.scratch_schema}}.mobile_base_duplicates_this_run`

The base module executes the incremental logic of the model - it retrieves all events for sessions with new data, deduplicates, and adds any enabled contexts.

The base module's 'complete' playbook (`99-base-complete.yml.tmpl`) updates the two relevant manifests, and cleans up temporary tables. The lifecycle of the `{{.scratch_schema}}.mobile_events_staged` table is completed by the `99-sessions-complete.yml.tmpl` step of the sessions module, when the table is truncated. This truncation can only occur during the completion step of the sessions module as `{{.scratch_schema}}.mobile_events_staged` is required as an input to the sessions module. This differs to the web model where the page views module's complete step would contain the truncation step.

The `{{.scratch_schema}}.mobile_events_this_run` table contains all events relevant only to this run of the model (since the last time the `99-base-complete.yml.tmpl` playbook has run). This table is dropped and recomputed _every_ run of the module, regardless of whether another module has used the data.

If there is a requirement that a custom module consumes data _more frequently than the screen views module for example_, the `{{.scratch_schema}}.mobile_events_this_run` table may be used for this purpose.

The `{{.scratch_schema}}.mobile_events_staged` table is incrementally updated to contain all events relevant to any run of the base module _since the last time the sessions module consumed it_ (ie since the last time the `99-sessions-complete.yml.tmpl` has run). This allows one to run the base module more frequently than the subsequent modules (if, for example, a custom module reads from events_this_run). 

Detail on configuring the base module's playbook can be found [in the relevant playbook directory's README](sql-runner/playbooks/standard/01-base).

### 02-screen-views

Inputs:             atomic tables, `{{.scratch_schema}}.mobile_events_staged`

Persistent Outputs: `{{.output_schema}}.mobile_screen_views`, `{{.scratch_schema}}.mobile_screen_views_staged`

Temporary Outputs:  `{{.scratch_schema}}.mobile_screen_views_this_run`

The screen views module takes `{{.scratch_schema}}.mobile_events_staged` as its input, joins in and deduplicates screen_view_id, calculates the standard mobile screen views model, and updates the production mobile_screen_views table. It also produces the `{{.scratch_schema}}.mobile_screen_views_staged` and `{{.scratch_schema}}.mobile_screen_views_this_run` tables.

The screen views module's 'complete' playbook `99-screen-views-complete.yml.tmpl` cleans up temporary tables. The lifecycle of the `{{.scratch_schema}}.mobile_screen_views_staged` table is completed by the `99-sessions-complete.yml.tmpl` step (of the subsequent module).

The `{{.scratch_schema}}.mobile_screen_views_this_run` table contains all events relevant only to this run of the model (since the last time the `99-screen-views-complete.yml.tmpl` playbook has run). This table is dropped and recomputed _every_ run of the module, regardless of whether another module has used the data.

If there is a requirement that a custom module consumes data _more frequently than the sessions module_, the `{{.scratch_schema}}.mobile_screen_views_this_run` table may be used for this purpose.

The `{{.scratch_schema}}.mobile_screen_views_staged` table is incrementally updated to contain all events relevant to any run of the screen views module _since the last time the sessions module consumed it_ (ie since the last time the `99-sessions-complete.yml.tmpl` playbook has run). This allows one to run the screen views module more frequently than the sessions module (if, for example, a custom module reads from mobile_screen_views_this_run).

Detail on configuring the screen views module's playbook can be found [in the relevant playbook directory's README](sql-runner/playbooks/02-screen-views).

### 03-optional-modules

#### 01-app-errors

Inputs:             atomic tables, `{{.scratch_schema}}.mobile_events_staged`

Persistent Outputs: `{{.output_schema}}.mobile_app_errors`, `{{.scratch_schema}}.mobile_app_errors_staged`

Temporary Outputs:  `{{.scratch_schema}}.mobile_app_errors_this_run`

The app errors module takes `{{.scratch_schema}}.mobile_events_staged` as its input, joins in the app errors context, calculates the app errors model, and updates the production mobile_app_errors table. It also produces the `{{.scratch_schema}}.mobile_app_errors_staged` and `{{.scratch_schema}}.mobile_app_errors_this_run` tables.

This crash reporting module is disabled by default since it is not a requirement to run the mobile model. Despite this, the `{{.scratch_schema}}.mobile_app_errors_staged` table will be created irrespectively. This is to allow the sessions module to run correctly where the `{{.scratch_schema}}.mobile_app_errors_staged` table is required as an input.

The app errors module's 'complete' playbook `99-app-errors-complete.yml.tmpl` cleans up temporary tables. The lifecycle of the `{{.scratch_schema}}.mobile_app_errors_staged` table is completed by the `99-sessions-complete.yml.tmpl` step (of the subsequent module).

The `{{.scratch_schema}}.mobile_app_errors_this_run` table contains all events relevant only to this run of the model (since the last time the `99-app-errors-complete.yml.tmpl` playbook has run). This table is dropped and recomputed _every_ run of the module, regardless of whether another module has used the data.

If there is a requirement that a custom module consumes data _more frequently than the sessions module_, the `{{.scratch_schema}}.mobile_app_errors_this_run` table may be used for this purpose.

The `{{.scratch_schema}}.mobile_app_errors_staged` table is incrementally updated to contain all events relevant to any run of the screen views module _since the last time the sessions module consumed it_ (ie since the last time the `99-sessions-complete.yml.tmpl` playbook has run). This allows one to run the app errors module more frequently than the sessions module (if, for example, a custom module reads from mobile_app_errors_this_run).

Detail on configuring the app errors module's playbook can be found [in the relevant playbook directory's README](sql-runner/playbooks/03-optional-modules/01-app-errors).

### 04-sessions

Inputs:             `{{.scratch_schema}}.mobile_screen_views_staged`, `{{.scratch_schema}}.mobile_app_errors_staged`, `{{.scratch_schema}}.mobile_events_staged`

Persistent Outputs: `{{.output_schema}}.mobile_sessions`, `{{.scratch_schema}}.mobile_sessions_userid_manifest_staged`

Temporary Outputs:  `{{.scratch_schema}}.mobile_sessions_this_run`

The sessions module takes the `_staged` output tables of the upstream modules as its input, calculates the standard sessions model, and updates the production sessions table. It also produces the `{{.scratch_schema}}.mobile_sessions_userid_manifest_staged` and `{{.scratch_schema}}.mobile_sessions_this_run{{.entropy}}` tables.

Unlike the other modules, the sessions module outputs a manifest of IDs as its staged table rather than a table containing all unprocessed data - this is due to the fact that the users step requires a longer lookback than the incremental structure contains, so there are obviously efficiency limitations.

The sessions module's 'complete' playbook `99-sessions-complete.yml.tmpl` truncates the input tables, and cleans up temporary tables. The lifecycle of the `{{.scratch_schema}}.mobile_sessions_userid_manifest_staged` table is completed by the `99-users-complete.yml.tmpl` step (of the subsequent module).

The `{{.scratch_schema}}.mobile_sessions_this_run` table contains all events relevant only to this run of the model (since the last time the `99-sessions-complete.yml.tmpl` playbook has run). This table is dropped and recomputed _every_ run of the module, regardless of whether another module has used the data.

If there is a requirement that a custom module consumes data _more frequently than the users module_, the `{{.scratch_schema}}.mobile_sessions_this_run` table may be used for this purpose.

The `{{.scratch_schema}}.mobile_sessions_userid_manifest_staged` table is incrementally updated to contain all IDs relevant to any run of the sessions module _since the last time the users module consumed it_ (ie since the last time the `99-users-complete.yml.tmpl` playbook has run). This allows one to run the sessions module more frequently than the users module (if, for example, a custom module reads from sessions_this_run and is more frequent than the page views module).

Detail on configuring the sessions module's playbook can be found [in the relevant playbook directory's README](sql-runner/playbooks/04-sessions).

### 05-users

Inputs:             `{{.scratch_schema}}.mobile_sessions_userid_manifest_staged`, `{{.output_schema}}.mobile_users_manifest`

Persistent Outputs: `{{.output_schema}}.mobile_users`

Temporary Outputs:  `{{.scratch_schema}}.mobile_users_this_run`

The sessions module takes `{{.scratch_schema}}.mobile_sessions_userid_manifest_staged` as its input, alongside the `{{.output_schema}}.mobile_users_manifest` table (which is self-maintained within the users module). It calculates the standard users model, and updates the production users table. It also produces the `{{.scratch_schema}}.mobile_users_this_run` table.

Unlike the other modules, the users module doesn't take an input that contains all information required to run the module. It uses the `{{.output_schema}}.mobile_users_manifest` table to manage efficiency, and queries the sessions table to process data as far back in history as is required.

The users module's 'complete' playbook `99-users-complete.yml.tmpl` truncates the `{{.scratch_schema}}.mobile_sessions_userid_manifest_staged` table, commits to the `{{.output_schema}}.mobile_users_manifest` and cleans up temporary tables. There is no `_staged` table for this module, as there are no subsequent modules.

The `{{.scratch_schema}}.mobile_users_this_run` table contains all events relevant only to this run of the model (since the last time the `99-users-complete.yml.tmpl` playbook has run). This table is dropped and recomputed _every_ run of the module, regardless of whether another module has used the data.

Detail on configuring the users module's playbook can be found [in the relevant playbook directory's README](sql-runner/playbooks/standard/05-users).

## Scheduling

### Asynchronous Runs

While the model is configured by default to run the entire way through, i.e. from the base module through to the users module, it is possible to run each module independently. For instance one could run the screen views module hourly while only running the sessions module daily. To do so you should run hourly all modules up to and including the desired module i.e. the base and screen view modules. The sessions module can then be run on a daily schedule. A few points to note:

- It is only when the sessions module is run that the `{{.scratch_schema}}.mobile_events_staged` is truncated. As a result, the hourly runs of the screen views module will both process new events data as well as re-process data stored in `mobile_events_staged` since the last time the sessions module ran. 
- Prior to running sessions module ensure that all input modules have been run i.e. base, screen views and _any enabled optional modules_. This ensures all the inputs are up to date and in-sync.

### Incomplete Runs

It is not a requirement to run every module. For example you may decide you do not need sessions or users data and only want screen view data. To do so:

- Set `stage_next` to `False` and `:ends_run:` to true in the screen views module. See the [README](sql-runner/playbooks/02-screen-views) for more details.
- Run all modules up to and including the screen views module. 
- Ensure that the sessions 'complete' playbook, `99-sessions-complete.yml.tmpl`, is the last step in the run. This playbook includes the truncation of the `mobile_events_staged` table. Without this truncation _each subsequent run will re-process data severely impacting performance._

## A note on duplicates

This version of the model (1.0.0) contains deduplication steps in both the base and screen views modules. The base module deduplicates on `event_id`, where _only_ the first row per `event_id` is kept (ordered by `collector_tstamp`).

The screen view module deduplicates on `screen_view_id`, where _only_ the first row per `screen_view_id` is kept (ordered by `derived_tstamp`).
