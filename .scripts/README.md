# Helper scripts

These scripts were created to simplify the process of running and testing the models. Their primary purpose is to provide a means of doing so during development without risking a credentials leak, and to avoid needing to run sql-runner once per playbook.

## Setup

The `run_playbooks`, `run_config`, `e2e` and `pr_check` scripts require SQL-runnner.

They also require that the relevant template database target in the `templates/` directory is populated. One can include the password in this template, or leave it as `PASSWORD_PLACEHOLDER` and authenticate by other means - see the authentication section below.

The `run_config`, `e2e`, and `pr_check` scripts require [jq](https://stedolan.github.io/jq/download/).

The `run_test`, `e2e`, and `pr_check` scripts require `python3`. To install dependencies:

```bash
cd data-models/.test
pip3 install -r requirements.txt
```

These scripts also require setting up a great_expectations datasource. To do so, run `great_expectations datasource new` from the `.test/` directory. This will create a configuration in `.test/great_expectations/config/config_variables.tml`. The entry for password can be replaced with `${REDSHIFT_PASSWORD}` to use script input or environment variables to authenticate.

## Authentication

### Passwords

These scripts allow for handling passwords via two means which reduce the risk of committing credentials to source control.

1. Set the `DB_PASSWORD` environment variable for Redshift and Snowflake, or the `BIGQUERY_CREDS` environment variable for Bigquery.

2. Pass the relevant credential to the relevant argument of the script in question.

### Other database details

Any script which uses SQL-runner will leverage the templates in `.scripts/templates/` to manage the database target connection details. Passwords should be left set to `PASSWORD_PLACEHOLDER`, but all other details should be hardcoded.

It's best to avoid committing these to source control - however doing so is less severe a risk than leaking a password.

## Usage

## run_config.sh

Runs a config json file (examples found in the `configs` folder of each model) - which specifies a list of playbooks to run.

Note that this script does not enforce dependencies, rather runs the playbooks in order of appearance. Snowplow Insights customers can take advantage of dependency resolution when running jobs on our Orchestration services.

**Arguments:**

```
-b (binary) path to sql-runner binary [required]
-c (config) path to config [required]
-a (auth) optional credentials for database target
-p (print SQL) use sql-runner fillTemplates to print pure sql
-d (dryRun) use sql-runner dry run
-o (output path) path to store output of sql-runner to sql file (to be used in conjunction with p)
-t (target template) path to target template to use (minimizes risk of credential leak)
```

**Examples:**

```bash
bash .scripts/run_config.sh -b ~/pathTo/sql-runner -c web/v1/bigquery/sql-runner/configs/datamodeling.json;

# Runs the standard bigquery web model end to end.

bash .scripts/run_config.sh -b ~/pathTo/sql-runner -c web/v1/bigquery/sql-runner/configs/datamodeling.json -d;

# Dry-runs the standard bigquery web model end to end.

bash .scripts/run_config.sh -b ~/pathTo/sql-runner -c web/v1/bigquery/sql-runner/configs/example_with_custom.json -p -o tmp/sql;

# Prints pure sql for the bigquery model and example custom steps to files in `tmp/sql` - with all templates filled in.
```

## run_test.sh

Runs a great_expectations suite.

The configuration for the tests can be found in the `expectations` directory.

We recommend using a virtual environment for python, eg. `pyenv` or `virtualenv` - for example using the latter:

```bash
virtualenv ~/myenv
source ~/myenv/bin/activate
```

Before running, make sure to install python requirements (python3 required):

```bash
cd data-models/.test
pip3 install -r requirements.txt
```

**Arguments:**

```
-d (database) target database for expectations [required]
-c (config) expectation config name [required]
-a (auth) optional credentials for database target
```

**Examples:**

```bash
bash .scripts/run_test.sh -d bigquery -c perm_tables;

# runs the perm_tables validation config against bigquery
```

## e2e.sh

`e2e.sh` runs a single end to end run of a standard model and great expectations tests.  

We recommend using a virtual environment for python, eg. `pyenv` or `virtualenv` - for example using the latter:

```bash
virtualenv ~/myenv
source ~/myenv/bin/activate
```

Before running, make sure to install python requirements (python3 required):

```bash
cd data-models/.test
pip3 install -r requirements.txt
```

**Arguments:**

```
-b (binary) path to sql-runner binary [required]
-d (database) target database for expectations [required]
-a (auth) optional credentials for database target
```

**Examples:**

```bash
bash .scripts/e2e.sh -b ~/pathTo/sql-runner -d bigquery;

# Runs the end to end testing script against bigquery
```

## pr_check.sh

Runs ten end to end runs of a standard model and tests. Exits on failure.

We recommend using a virtual environment for python, eg. `pyenv` or `virtualenv` - for example using the latter:

```bash
virtualenv ~/myenv
source ~/myenv/bin/activate
```

Before running, make sure to install python requirements (python3 required):

```bash
cd data-models/.test
pip3 install -r requirements.txt
```

**Arguments:**

```
-b (binary) path to sql-runner binary [required]
-d (database) target database for expectations [required]
-a (auth) optional credentials for database target
```

**Examples:**

```bash
bash .scripts/pr_check.sh -b ~/pathTo/sql-runner -d bigquery;

# Runs the pr check testing script against bigquery
```

### `run_playbooks.sh` (deprecated)

Deprecated - `run_config.sh` provides a simpler instrumentation for this functionality.

Runs a list of playbooks in sequence, using sql-runnner.

**Arguments:**

```bash
bash run_playbooks.sh {path_to_sql_runner} {database} {major version} '{list_of_playbooks_no_extension},{comma_separated}' {credentials (optional)}


# {path_to_sql_runner} - Path to your local instance of SQL-runner
# {database} - Database to run (`redshift`, `snowflake` or `bigquery` - note that only redshift is currently implemented)
# {major version} - Version of the model to run (according to the directory that houses it - eg. `v0` or `v1`)
# '{list_of_playbooks_no_extension},{comma_separated}' - A string containing a list of playbook paths, from the 'playbooks' folder, with no file extension (eg. `standard/00-setup/00-setup-metadata,standard/01-base/01-base-main`).
# {credentials (optional)} - Credentials for the database (optional, this can be provided by env var also)
```

**Examples:**

```bash
bash .scripts/run_playbooks.sh ~/sql-runner redshift v1 'standard/01-base/01-base-main,standard/02-page-views/01-page-views-main,standard/03-sessions/01-sessions-main,standard/04-users/01-users-main';

# Runs the base, page views, sessions and users main playbooks for redshift
```
