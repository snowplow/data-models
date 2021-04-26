# Configuring the 00-staging-reconciliation playbooks

The staging reconciliation module reconciles all the `_staging` output tables from the standard modules. It outputs to a scratch `mobile_staging_reconciliation` table which is then validated using Great Expectations. For all tests to pass, every columns in `mobile_staging_reconciliation` must equal 0.

`01-staging-reconciliation-main.yml.tmpl` runs the main reconciliation logic. `99-staging-reconciliation-complete.yml.tmpl` drops the scratch table.

## Configuration quick reference

### 01-staging-reconciliation-main

`:scratch_schema:`     name of scratch schema  

`:entropy:`            string to append to all tables, to test without affecting prod tables (eg. `_test` produces tables like `mobile_users_test`). Must match entropy value used for all other modules in a given run.

`:app_errors:`       boolean- default false. Set to true if the App Errors module is enabled within the main mobile model. By enabling, the `_staged` output of the App Errors module is checked as part of the reconciliation.

### 99-staging-reconciliation-complete

`:scratch_schema:`     name of scratch schema

`:entropy:`            string to append to all tables, to test without affecting prod tables. Must match entropy value used for all other modules in a given run.

`:cleanup_mode:`       options: `debug` - only keeps main tables. `trace` - keeps all tables. `all` - cleans up everything.

