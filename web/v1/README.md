# Web model v1

## Quickstart

### Prerequisites

[SQL-runner](https://github.com/snowplow/sql-runner) must be installed, and a dataset of web events from the [Snowplow Javascript tracker](https://docs.snowplowanalytics.com/docs/collecting-data/collecting-from-own-applications/javascript-tracker/) must be available in the database.

### Configuration

First, fill in the connection details for the target database in the relevant template in `.scripts/template/redshift_template.yml.tmpl`.

Password can be left as a `PASSWORD_PLACEHOLDER`, and set as an environment variable or passed as an argument to the run_playbooks script. See the README in `.scripts` for more detail.

Variables in each module's playbook can also optionally be configured also. See each playbook directory's README for more detail on configuration of each module.

### Run using the `run_playbooks.sh` script

To run the entire model, end to end (for redshift):

```bash
bash data-models/.scripts/run_playbooks.sh {path_to_sql_runner} {database} {major version} 'standard/01-base/01-base-main,standard/02-page-views/01-page-views-main,standard/03-sessions/01-sessions-main,standard/04-users/01-users-main,standard/01-base/99-base-complete,standard/02-page-views/99-page-views-complete,standard/03-sessions/99-sessions-complete,standard/04-users/99-users-complete' {credentials (optional)};
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

To run the test suite alone:

```bash
bash run_test.sh {database} {major version} {validation_config} {credentials (optional)}
```

To run an entire run of the standard model, and tests end to end:

```bash
bash e2e.sh {path_to_sql_runner} {database} {major version} {credentials (optional)}
```

To run a full battery of ten runs of the standard model, and tests:

```bash
bash pr_check.sh {path_to_sql_runner} {database} {major version} {credentials (optional)}
```

### Adding to tests

Check out the [Great Expectations documentation](https://docs.greatexpectations.io/en/latest/) for guidance on using it to run existing test suites directly, create new expectations, use the profiler, and autogenerate data documentation.

Quickstart to create a new test suite:

`great_expectations suite new`
