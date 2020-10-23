# Helper scripts

These scripts were created to simplify the process of running and testing the models. Their primary purpose is to provide a means of doing so during development without risking a credentials leak, and to avoid needing to run sql-runner once per playbook.

## Setup

The `run_playbooks`, `e2e` and `pr-check` scripts require SQL-runnner.

They also require that the relevant template database target in the `templates/` directory is populated. One can include the password in this template, or leave it as `PASSWORD_PLACEHOLDER` and authenticate by other means - see the authentication section below.

The `run_test`, `e2e` and `pr-check` scripts require `python3`. To install dependencies:

`cd data-models/.test`
`pip3 install -r requirements.txt`

These scripts also require setting up a great_expectations datasource. To do so, run `great_expectations datasource new` from the `.test/` directory. This will create a configuration in `.test/great_expectations/config/config_variables.tml`. The entry for password can be replaced with `${REDSHIFT_PASSWORD}` to use script input or environment variables to authenticate.

## Authentication

These scripts allow for handling passwords via two means which reduce the risk of committing credentials to source control.

One may either set the variable `REDSHIFT_PASSWORD` or pass it into `credentials` argument of the relevant script. In both cases the relevant credential will be written to temporary files in order to run the script, and these will be removed on exit.

For great_expectations authentication, after configuring a datasource, set the entry for password in `.test/great_expectations/config/config_variables.tml` to `${REDSHIFT_PASSWORD}`.

It is also possible to set passwords directly in the relevant configuration, however this adds scope for an accidental credentials leak.

## Usage

### `run_playbooks.sh`

`run_playbooks` runs a list of playbooks in sequence, using sql-runnner.

Before running, make sure to populate the relevant target template in the `.scripts/templates` directory.

```bash
bash run_playbooks.sh {path_to_sql_runner} {database} {major version} '{list_of_playbooks_no_extension},{comma_separated}' {credentials (optional)}
```

The arguments are:

`{path_to_sql_runner}` - Path to your local instance of SQL-runner
`{database}` - Database to run (`redshift`, `snowflake` or `bigquery` - note that only redshift is currently implemented)
`{major version}` - Version of the model to run (according to the directory that houses it - eg. `v0` or `v1`)
`'{list_of_playbooks_no_extension},{comma_separated}'` - A string containing a list of playbook paths, from the 'playbooks' folder, with no file extension (eg. `standard/00-setup/00-setup-metadata,standard/01-base/01-base-main`).
`{credentials (optional)}` - Credentials for the database (optional, this can be provided by env var also)

For example:

```bash
bash .scripts/run_playbooks.sh ~/sql-runner redshift v1 'standard/01-base/01-base-main,standard/02-page-views/01-page-views-main,standard/03-sessions/01-sessions-main,standard/04-users/01-users-main' abcd1234;
```

## run_test.sh

`run_test.sh` runs a great_expectations test suite on the output of the model. The configuration for the tests can be found in the `expectations` directory. Currently, the tests are configured to run on both temporary and permanent tables, and so should be run before the cleanup step of a model, or on a model that has `cleanup_mode` configure to `debug` or `trace`.

Before running, make sure to install python requirements (python3 required):

`cd data-models/.test`
`pip3 install -r requirements.txt`

```bash
bash run_test.sh {database} {major version} {validation_config} {credentials (optional)}
```

The arguments are:

`{database}` - Database to run (`redshift`, `snowflake` or `bigquery` - note that only redshift is currently implemented)
`{major version}` - Version of the model to test (according to the directory that houses it - eg. `v0` or `v1`)
`{validation_config}` - Name of the validation config to run. (Options can be found in the `.tests/great_expectations/` directory)
`{credentials (optional)}` - Credentials for the database (optional, this can be provided by env var also)

## e2e.sh

`e2e.sh` runs a complete single run of a model, along with the tests for that model. This happens in two steps - first the main steps run and the `temp_tables` validation config is run, then the `complete` steps run, and the `perm_tables` validation config is run.

```bash
bash e2e.sh {path_to_sql_runner} {database} {major version} {credentials (optional)}
```

The arguments are:

`{path_to_sql_runner}` - Path to your local instance of SQL-runner
`{database}` - Database to run (`redshift`, `snowflake` or `bigquery` - note that only redshift is currently implemented)
`{major version}` - Version of the model to run (according to the directory that houses it - eg. `v0` or `v1`)
`{credentials (optional)}` - Credentials for the database (optional, this can be provided by env var also)

## pr-check.sh

`pr-check.sh` runs `e2e.sh` ten times. Because there are certain types of bugs and anomalies in incremental data modeling which only manifest themselves after several runs of the model, we run the tests many times before any release. It is expected that anyone using this script is working with a dataset of a manageable size for their cost tolerance. The amount of data that any run of the model processes can be configured using playbook variables.

```bash
bash pr-check.sh {path_to_sql_runner} {database} {major version} {credentials (optional)}
```

The arguments are:

`{path_to_sql_runner}` - Path to your local instance of SQL-runner
`{database}` - Database to run (`redshift`, `snowflake` or `bigquery` - note that only redshift is currently implemented)
`{major version}` - Version of the model to run (according to the directory that houses it - eg. `v0` or `v1`)
`{credentials (optional)}` - Credentials for the database (optional, this can be provided by env var also)
