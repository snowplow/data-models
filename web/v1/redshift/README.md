# Redshift v1 model README

This readme contains a quickstart guide, and details of how the modules interact with each other. For a guide to configuring each module, there is a README in each of the modules' `playbooks` directory.

To customise the model, we recommend following the guidance found in the README in the `sql/custom` directory.

## Quickstart

### Prerequisites

[SQL-runner](https://github.com/snowplow/sql-runner) must be installed, and a dataset of web events from the [Snowplow Javascript tracker](https://docs.snowplowanalytics.com/docs/collecting-data/collecting-from-own-applications/javascript-tracker/) must be available in the database.

### Configuration

First, fill in the connection details for the target database in the relevant template in `.scripts/template/redshift_template.yml.tmpl`.

Password can be left as a `PASSWORD_PLACEHOLDER`, and set as an environment variable or passed as an argument to the run_playbooks script. See the README in `.scripts` for more detail.

Variables in each module's playbook can also optionally be configured also. See each playbook directory's README for more detail on configuration of each module.

### Run using the `run_playbooks.sh` script

To run the entire model, end to end:

```bash
bash data-models/.scripts/run_playbooks.sh {path_to_sql_runner} redshift v1 'standard/01-base/01-base-main,standard/02-page-views/01-page-views-main,standard/03-sessions/01-sessions-main,standard/04-users/01-users-main,standard/01-base/99-base-complete,standard/02-page-views/99-page-views-complete,standard/03-sessions/99-sessions-complete,standard/04-users/99-users-complete' {credentials (optional)};
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

### Using the helper scripts

To run the test suites alone:

```bash
bash run_test.sh redshift v1 perm_tables {credentials (optional)}
bash run_test.sh redshift v1 temp_tables {credentials (optional)}
```

To run an entire run of the standard model, and tests end to end:

```bash
bash e2e.sh {path_to_sql_runner} redshift v1 {credentials (optional)}
```

To run a full battery of ten runs of the standard model, and tests:

```bash
bash pr_check.sh {path_to_sql_runner} redshift v1 {credentials (optional)}
```

### Adding to tests

Check out the [Great Expectations documentation](https://docs.greatexpectations.io/en/latest/) for guidance on using it to run existing test suites directly, create new expectations, use the profiler, and autogenerate data documentation.

Quickstart to create a new test suite:

`great_expectations suite new`

## Modules detail

### 01-base

Inputs:             atomic tables, `{{.output_schema}}.base_event_id_manifest`, `{{.output_schema}}.base_session_id_manifest`

Persistent Outputs: `{{.scratch_schema}}.events_staged`,

Temporary Outputs:  `{{.scratch_schema}}.events_this_run`, `{{.scratch_schema}}.base_duplicates_this_run`

The base module executes the incremental logic of the model - it retrieves all events for sessions with new data, deduplicates, and adds the page_view_id.

The base module's 'complete' playbook (`99-base-complete.yml.tmpl`) updates the two relevant manifests, and cleans up temporary tables. The lifecycle of the `{{.scratch_schema}}.events_staged` table is completed by the `99-page-views-complete.yml.tmpl` step (of the subsequent module).

A record of the duplicates removed for the run is logged in the `{{.scratch_schema}}.base_duplicates_this_run` table. Note that the base_duplicates_this_run table is dropped and recomputed every run, users interested in permanently logging them should create a custom module to handle this.

The `{{.scratch_schema}}.events_this_run` table contains all events relevant only to this run of the model (since the last time the `99-base-complete.yml.tmpl` playbook has run). This table is dropped and recomputed _every_ run of the module, regardless of whether another module has used the data.

If there is a requirement that a custom module consumes data _more frequently than the page views module_, the `{{.scratch_schema}}.events_this_run` table may be used for this purpose.

The `{{.scratch_schema}}.events_staged` table is incrementally updated to contain all events relevant to any run of the base module _since the last time the page views module consumed it_ (ie since the last time the `99-page-views-complete.yml.tmpl` has run). This allows one to run the base module more frequently than the page views module (if, for example, a custom module reads from events_this_run).

Detail on configuring the base module's playbook can be found [in the relevant playbook directory's README](sql-runner/playbooks/standard/01-base).

### 02-page-views

Inputs:             atomic tables, `{{.scratch_schema}}.events_staged`

Persistent Outputs: `{{.output_schema}}.page_views`, `{{.scratch_schema}}.page_views_staged`

Temporary Outputs:  `{{.scratch_schema}}.page_views_this_run`, `{{.scratch_schema}}.pv_page_view_id_duplicates_this_run`

The page views module takes `{{.scratch_schema}}.events_staged` as its input, deduplicates on page_view_id, calculates the standard page views model, and updates the production page_views table. It also produces the `{{.scratch_schema}}.page_views_staged` and `{{.scratch_schema}}.page_views_this_run` tables.

The page views module's 'complete' playbook `99-page-views-complete.yml.tmpl` truncates the `{{.scratch_schema}}.events_staged` table, and cleans up temporary tables. The lifecycle of the `{{.scratch_schema}}.page_views_staged` table is completed by the `99-sessions-complete.yml.tmpl` step (of the subsequent module).

A record of the duplicates removed for the run is logged in the `{{.scratch_schema}}.pv_page_view_id_duplicates_this_run` table. Note that the `{{.scratch_schema}}.pv_page_view_id_duplicates_this_run` table is dropped and recomputed every run, users interested in permanently logging them should create a custom module to handle this.

The `{{.scratch_schema}}.page_views_this_run` table contains all events relevant only to this run of the model (since the last time the `99-page-views-complete.yml.tmpl` playbook has run). This table is dropped and recomputed _every_ run of the module, regardless of whether another module has used the data.

If there is a requirement that a custom module consumes data _more frequently than the sessions module_, the `{{.scratch_schema}}.page_views_this_run` table may be used for this purpose.

The `{{.scratch_schema}}.page_views_staged` table is incrementally updated to contain all events relevant to any run of the page views module _since the last time the sessions module consumed it_ (ie since the last time the `99-sessions-complete.yml.tmpl` playbook has run). This allows one to run the page views module more frequently than the sessions module (if, for example, a custom module reads from page_views_this_run).

The page views module also contains optional add-on steps. These can be configured to run or not based on which enrichments the user has enabled, and wishes to include in the model.

Detail on configuring the page views module's playbook can be found [in the relevant playbook directory's README](sql-runner/playbooks/02-page-views).

### 03-sessions

Inputs:             `{{.scratch_schema}}.page_views_staged`

Persistent Outputs: `{{.output_schema}}.sessions`, `{{.scratch_schema}}.sessions_userid_manifest_staged`

Temporary Outputs:  `{{.scratch_schema}}.sessions_this_run`

The sessions module takes `{{.scratch_schema}}.page_views_staged` as its input, calculates the standard sessions model, and updates the production sessions table. It also produces the `{{.scratch_schema}}.sessions_userid_manifest_staged` and `{{.scratch_schema}}.sessions_this_run{{.entropy}}` tables.

Unlike the other modules, the sessions module outputs a manifest of IDs as its staged table rather than a table containing all unprocessed data - this is due to the fact that the users step requires a longer lookback than the incremental structure contains, so there are obviously efficiency limitations.

The sessions module's 'complete' playbook `99-sessions-complete.yml.tmpl` truncates the `{{.scratch_schema}}.page_views_staged` table, and cleans up temporary tables. The lifecycle of the `{{.scratch_schema}}.sessions_userid_manifest_staged` table is completed by the `99-users-complete.yml.tmpl` step (of the subsequent module).

The `{{.scratch_schema}}.sessions_this_run` table contains all events relevant only to this run of the model (since the last time the `99-sessions-complete.yml.tmpl` playbook has run). This table is dropped and recomputed _every_ run of the module, regardless of whether another module has used the data.

If there is a requirement that a custom module consumes data _more frequently than the users module_, the `{{.scratch_schema}}.sessions_this_run` table may be used for this purpose.

The `{{.scratch_schema}}.sessions_userid_manifest_staged` table is incrementally updated to contain all IDs relevant to any run of the sessions module _since the last time the users module consumed it_ (ie since the last time the `99-users-complete.yml.tmpl` playbook has run). This allows one to run the sessions module more frequently than the users module (if, for example, a custom module reads from sessions_this_run and is more frequent than the page views module).

Detail on configuring the sessions module's playbook can be found [in the relevant playbook directory's README](sql-runner/playbooks/03-sessions).

### 04-users

Inputs:             `{{.scratch_schema}}.sessions_userid_manifest_staged`, `{{.output_schema}}.users_manifest`

Persistent Outputs: `{{.output_schema}}.users`

Temporary Outputs:  `{{.scratch_schema}}.users_this_run`

The sessions module takes `{{.scratch_schema}}.sessions_userid_manifest_staged` as its input, alongside the `{{.output_schema}}.users_manifest` table (which is self-maintained within the users module). It calculates the standard users model, and updates the production users table. It also produces the `{{.scratch_schema}}.users_this_run` table.

Unlike the other modules, the users module doesn't take an input that contains all information required to run the module. It uses the `{{.output_schema}}.users_manifest` table to manage efficiency, and queries the sessions table to process data as far back in history as is required.

The users module's 'complete' playbook `99-users-complete.yml.tmpl` truncates the `{{.scratch_schema}}.sessions_userid_manifest_staged` table, commits to the `{{.output_schema}}.users_manifest` and cleans up temporary tables. There is no `_staged` table for this module, as there are no subsequent modules.

The `{{.scratch_schema}}.users_this_run` table contains all events relevant only to this run of the model (since the last time the `99-users-complete.yml.tmpl` playbook has run). This table is dropped and recomputed _every_ run of the module, regardless of whether another module has used the data.

Detail on configuring the users module's playbook can be found [in the relevant playbook directory's README](sql-runner/playbooks/04-users).

## A note on duplicates

This version of the model (1.1.0) excludes duplicated event_ids and page_view_ids. Ideally in the future it will provide standard options for handling them, however because customisations of this model involve queries on federated atomic tables, the safest option for a first version is to exclude them.

If there is a need to handle duplicates, this can be done by adding a custom module to the base level of aggregation - take good care to manage the possibility of introducing duplicates downstream if doing so.

Normally, one would expect less than 1% duplicates in the dataset. If the requirement to handle duplicates arises from the fact that there is a large proportion of them, users are advised to first investigate the source of duplicates and attempt to address the issue upstream of the data models - a high proportion of duplicates can be indicative of a more significant issue in tracking or configuration of the pipeline.
