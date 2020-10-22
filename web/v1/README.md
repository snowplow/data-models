# Web model v1

## Quickstart guide

### Prerequisites

[SQL-runner](https://github.com/snowplow/sql-runner) must be installed, and a dataset of web events from the [Snowplow Javascript tracker](https://docs.snowplowanalytics.com/docs/collecting-data/collecting-from-own-applications/javascript-tracker/) must be available in the database.

### Configuration

First, fill in the connection details for the target database in the relevant template in `.scripts/template/redshift_template.yml.tmpl`.

Password can be left as a `PASSWORD_PLACEHOLDER`, and set as an environment variable or passed as an argument to the run_playbooks script. See the README in `.scripts` for more detail.

Variables in each module's playbook can also optionally be configured also. See each playbook directory's README for more detail on configuration of each module.

### Run using the `rub_playbooks.sh` script

To run the entire model, end to end (for redshift):

```bash
bash data-models/.scripts/run_playbooks.sh {path_to_sql_runner} {database} {major version} 'standard/01-base/01-base-main,standard/02-page-views/01-page-views-main,standard/03-sessions/01-sessions-main,standard/04-users/01-users-main,standard/01-base/99-base-complete,standard/02-page-views/99-page-views-complete,standard/03-sessions/99-sessions-complete,standard/04-users/99-users-complete' {credentials (optional)};
```

See the README in the `.scripts/` directory for more details.
