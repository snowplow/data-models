# Snowflake v1 model README

This readme contains a quickstart guide, and details of how the modules interact with each other. For a guide to configuring each module, there is a README in each of the modules' `playbooks` directory.

To customise the model, we recommend following the guidance found in the [README](./sql-runner/sql/custom/README.md) in the `sql/custom` directory.

## Quickstart

### Prerequisites

[SQL-runner](https://github.com/snowplow/sql-runner) must be installed, and a dataset of web events from the [Snowplow Javascript tracker](https://docs.snowplowanalytics.com/docs/collecting-data/collecting-from-own-applications/javascript-tracker/) must be available in the database.

**Note:** For the Snowflake web model, SQL Runner v0.9.3 or above is required.

### Configuration

1. Fill in the connection parameters for the Snowflake target in the relevant [template](../../../.scripts/templates/snowflake.yml.tmpl) (`.scripts/template/snowflake.yml.tmpl`). It is suggested that the password is left as `PASSWORD_PLACEHOLDER` and set as an environment variable **or** passed as an argument to the scripts. See this [README](../../../.scripts/README.md) for more detail.

2. Check the values of the required variables in the [playbooks](./sql-runner/playbooks). More details on the variables configuring each module can be found in the corresponding README's throughout the directory structure:

```
playbooks/
├── custom
│   ├── 02-page-views-join
│   │   ├── 01-page-views-join.yml.tmpl
│   │   ├── 99-page-views-join-complete.yml.tmpl
│   │   └── XX-destroy-page-views-join.yml.tmpl
│   └── README.md
└── standard
    ├── 00-setup
    │   ├── 00-setup-metadata.yml.tmpl
    │   ├── 99-metadata-complete.yml.tmpl
    │   ├── README.md
    │   └── XX-destroy-metadata.yml.tmpl
    ├── 01-base
    │   ├── 01-base-main.yml.tmpl
    │   ├── 99-base-complete.yml.tmpl
    │   ├── README.md
    │   └── XX-destroy-base.yml.tmpl
    ├── 02-page-views
    │   ├── 01-page-views-main.yml.tmpl
    │   ├── 99-page-views-complete.yml.tmpl
    │   ├── README.md
    │   └── XX-destroy-page-views.yml.tmpl
    ├── 03-sessions
    │   ├── 01-sessions-main.yml.tmpl
    │   ├── 99-sessions-complete.yml.tmpl
    │   ├── README.md
    │   └── XX-destroy-sessions.yml.tmpl
    └── 04-users
        ├── 01-users-main.yml.tmpl
        ├── 99-users-complete.yml.tmpl
        ├── README.md
        └── XX-destroy-users.yml.tmpl
```

### Run using the `run_config.sh` script

To run the entire standard model, end to end:

```bash
bash .scripts/run_config.sh -b ~/pathTo/sql-runner -c web/v1/snowflake/sql-runner/configs/datamodeling.json -t .scripts/templates/snowflake.yml.tmpl;
```

See the README in the `.scripts/` directory for more details.

## Custom Modules

A guide to creating custom modules can be found in the [README](./sql-runner/sql/custom/README.md) of the `sql/custom/` directory of the model. Each custom module created must consist of a set of sql files and playbook(s).

An example of a datamodeling configuration which includes custom steps can be found [here](./sql-runner/configs/example_with_custom.json).

## Testing

### Setup

Python3 is required.

We recommend using a virtual environment for python, eg. `pyenv` or `virtualenv` - for example using the latter:

```bash
virtualenv ~/myenv
source ~/myenv/bin/activate
```

Install [Great Expectations](https://greatexpectations.io/), and configure a datasource:

```bash
cd .test
pip3 install -r requirements.txt
great_expectations datasource new
```

Follow the CLI guide to configure access to your database. The configuration for your datasource will be generated in `.test/great_expectations/config/config_variables.tml` - these values can be replaced by environment variables if desired.

### Using the helper scripts

To run the test suites alone:

```bash
bash .scripts/run_test.sh -d snowflake -c temp_tables;
bash .scripts/run_test.sh -d snowflake -c perm_tables;
```

To run an entire run of the standard model, and tests end to end:

```bash
bash .scripts/e2e.sh -b ~/pathTo/sql-runner -d snowflake;
```

To run a full battery of ten runs of the standard model, and tests:

```bash
bash .scripts/pr_check.sh -b ~/pathTo/sql-runner -d snowflake;
```

### Adding to tests

Check out the [Great Expectations documentation](https://docs.greatexpectations.io/en/latest/) for guidance on using it to run existing test suites directly, create new expectations, use the profiler, and autogenerate data documentation.

Quickstart to create a new test suite:

```bash
great_expectations suite new
```

## Standard modules

### 01-base

 | **Inputs**                                            | **Temporary Outputs**                                 | **Persistent Outputs**                                |
 | ----------------------------------------------------- | ----------------------------------------------------- | ----------------------------------------------------- |
 | `atomic.events`                                       | `scratch.events_this_run`                             | `scratch.events_staged`                               |
 | `derived.base_event_id_manifest`                      | `scratch.base_duplicates_this_run`                    |                                                       |
 | `derived.base_session_id_manifest`                    |                                                       |                                                       |


The base module executes the incremental logic of the model - it retrieves all events for sessions with new data, deduplicates on `event_id`, and extracts the `page_view_id` from the webpage context.

The base module's 'complete' playbook (`99-base-complete.yml.tmpl`) updates the two relevant manifests, and cleans up temporary tables. The lifecycle of the `{{.scratch_schema}}.events_staged` table is completed by the `99-page-views-complete.yml.tmpl` step (of the subsequent module).

A record of the duplicates removed for the run is logged in the `{{.scratch_schema}}.base_duplicates_this_run` table. Note that the `base_duplicates_this_run` table is dropped and recomputed every run. Users interested in permanently logging them should create a custom module to handle this.

The `{{.scratch_schema}}.events_this_run` table contains all events relevant only to this run of the model (since the last time the `99-base-complete.yml.tmpl` playbook has run). This table is dropped and recomputed _every_ run of the module, regardless of whether another module has used the data.

If there is a requirement that a custom module consumes data _more frequently than the page views module_, the `{{.scratch_schema}}.events_this_run` table may be used for this purpose.

The `{{.scratch_schema}}.events_staged` table is incrementally updated to contain all events relevant to any run of the base module _since the last time the page views module consumed it_ (ie since the last time the `99-page-views-complete.yml.tmpl` has run). This allows one to run the base module more frequently than the page views module (if, for example, a custom module reads from events_this_run).

Detail on configuring the base module's playbook can be found [in the relevant playbook directory's README](./sql-runner/playbooks/standard/01-base/README.md).

### 02-page-views

 | **Inputs**              | **Temporary Outputs**                         | **Persistent Outputs**      |
 | ----------------------- | --------------------------------------------- | --------------------------- |
 | `scratch.events_staged` | `scratch.page_views_this_run`                 | `derived.page_views`        |
 |                         | `scratch.pv_page_view_id_duplicates_this_run` | `scratch.page_views_staged` |


The page views module takes `{{.scratch_schema}}.events_staged` as its input, deduplicates on `page_view_id`, calculates the standard page views model, and updates the production page_views table. It also produces the `{{.scratch_schema}}.page_views_staged` and `{{.scratch_schema}}.page_views_this_run` tables.

The page views module's 'complete' playbook `99-page-views-complete.yml.tmpl` truncates the `{{.scratch_schema}}.events_staged` table, and cleans up temporary tables. The lifecycle of the `{{.scratch_schema}}.page_views_staged` table is completed by the `99-sessions-complete.yml.tmpl` step (of the subsequent module).

A record of the duplicates removed for the run is logged in the `{{.scratch_schema}}.pv_page_view_id_duplicates_this_run` table. Note that the `{{.scratch_schema}}.pv_page_view_id_duplicates_this_run` table is dropped and recomputed every run. Users interested in permanently logging them should create a custom module to handle this.

The `{{.scratch_schema}}.page_views_this_run` table contains all events relevant only to this run of the model (since the last time the `99-page-views-complete.yml.tmpl` playbook has run). This table is dropped and recomputed _every_ run of the module, regardless of whether another module has used the data.

If there is a requirement that a custom module consumes data _more frequently than the sessions module_, the `{{.scratch_schema}}.page_views_this_run` table may be used for this purpose.

The `{{.scratch_schema}}.page_views_staged` table is incrementally updated to contain all events relevant to any run of the page views module _since the last time the sessions module consumed it_ (ie since the last time the `99-sessions-complete.yml.tmpl` playbook has run). This allows one to run the page views module more frequently than the sessions module (if, for example, a custom module reads from page_views_this_run).

The page views module also contains optional add-on steps. These can be configured to run or not based on which enrichments the user has enabled, and wishes to include in the model.

Detail on configuring the page views module's playbook can be found [in the relevant playbook directory's README](./sql-runner/playbooks/standard/02-page-views/README.md).

### 03-sessions

 | **Inputs**                  | **Temporary Outputs**       | **Persistent Outputs**                    |
 | --------------------------- | --------------------------- | ----------------------------------------- |
 | `scratch.page_views_staged` | `scratch.sessions_this_run` | `derived.sessions`                        |
 |                             |                             | `scratch.sessions_userid_manifest_staged` |

The sessions module takes `{{.scratch_schema}}.page_views_staged` as its input, calculates the standard sessions model, and updates the production sessions table. It also produces the `{{.scratch_schema}}.sessions_userid_manifest_staged` and `{{.scratch_schema}}.sessions_this_run{{.entropy}}` tables.

Unlike the other modules, the sessions module outputs a manifest of IDs as its staged table rather than a table containing all unprocessed data - this is due to the fact that the users step requires a longer lookback than the incremental structure contains, so there are obviously efficiency limitations.

The sessions module's 'complete' playbook `99-sessions-complete.yml.tmpl` truncates the `{{.scratch_schema}}.page_views_staged` table, and cleans up temporary tables. The lifecycle of the `{{.scratch_schema}}.sessions_userid_manifest_staged` table is completed by the `99-users-complete.yml.tmpl` step (of the subsequent module).

The `{{.scratch_schema}}.sessions_this_run` table contains all events relevant only to this run of the model (since the last time the `99-sessions-complete.yml.tmpl` playbook has run). This table is dropped and recomputed _every_ run of the module, regardless of whether another module has used the data.

If there is a requirement that a custom module consumes data _more frequently than the users module_, the `{{.scratch_schema}}.sessions_this_run` table may be used for this purpose.

The `{{.scratch_schema}}.sessions_userid_manifest_staged` table is incrementally updated to contain all IDs relevant to any run of the sessions module _since the last time the users module consumed it_ (ie since the last time the `99-users-complete.yml.tmpl` playbook has run). This allows one to run the sessions module more frequently than the users module (if, for example, a custom module reads from sessions_this_run and is more frequent than the page views module).

Detail on configuring the sessions module's playbook can be found [in the relevant playbook directory's README](./sql-runner/playbooks/standard/03-sessions/README.md).

### 04-users

 | **Inputs**                                | **Temporary Outputs**    | **Persistent Outputs**  |
 | ----------------------------------------- | ------------------------ | ----------------------- |
 | `scratch.sessions_userid_manifest_staged` | `scratch.users_this_run` | `derived.users`         |
 | `derived.users_manifest`                  | `scratch.users_limits`   |                         |

The users module takes `{{.scratch_schema}}.sessions_userid_manifest_staged` as its input, alongside the `{{.output_schema}}.users_manifest` table (which is self-maintained within the users module). It calculates the standard users model, and updates the production users table. It also produces the `{{.scratch_schema}}.users_this_run` table.

Unlike the other modules, the users module doesn't take an input that contains all information required to run the module. It uses the `{{.output_schema}}.users_manifest` table to manage efficiency, and queries the sessions table to process data as far back in history as is required.

The users module's 'complete' playbook `99-users-complete.yml.tmpl` truncates the `{{.scratch_schema}}.sessions_userid_manifest_staged` table, commits to the `{{.output_schema}}.users_manifest` and cleans up temporary tables. There is no `_staged` table for this module, as there are no subsequent modules.

The `{{.scratch_schema}}.users_this_run` table contains all events relevant only to this run of the model (since the last time the `99-users-complete.yml.tmpl` playbook has run). This table is dropped and recomputed _every_ run of the module, regardless of whether another module has used the data.

Detail on configuring the users module's playbook can be found [in the relevant playbook directory's README](./sql-runner/playbooks/standard/04-users/README.md).


## A note on duplicates

This version of the model (1.0.0) excludes duplicated `event_id`s and `page_view_id`s. Ideally in the future it will provide standard options for handling them.

If there is a need to handle duplicates, this can be done by adding a custom module to the base level of aggregation - take good care to manage the possibility of introducing duplicates downstream if doing so.

Normally, one would expect less than 1% duplicates in the dataset. If the requirement to handle duplicates arises from the fact that there is a large proportion of them, users are advised to first investigate the source of duplicates and attempt to address the issue upstream of the data models - a high proportion of duplicates can be indicative of a more significant issue in tracking or configuration of the pipeline.


## A note on Constraints and Clustering keys

This 1.0.0 version of the Snowflake web model does not use Constraints or Clustering keys in the table definitions, even though it could.

Concerning [clustering keys](https://docs.snowflake.com/en/user-guide/tables-clustering-keys.html#strategies-for-selecting-clustering-keys), Snowflake's naturally clusters the tables on insertion order, and there hasn't been evidence so far suggesting a change towards another manual clustering strategy.

Concerning table [constraints](https://docs.snowflake.com/en/sql-reference/constraints-overview.html), it is a fact that Snowflake enforces **only** the `NOT NULL` constraint. Therefore, in this 1.0.0 version we decided to include only this constraint that is actually enforced, for clarity on the model's assumptions.
