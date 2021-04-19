# Stored procedures

Below you can read about stored procedured defined for the Snowflake modules. They are defined under the `{{.output_schema}}` and you can find the source code [here](./01-main/01-stored-procedures.sql).

For modularity, they are recreated in the first step of all `01-main` playbooks and they are explicitly dropped in the `XX-destroy` steps.

Snowflake's [stored procedures](https://docs.snowflake.com/en/sql-reference/stored-procedures.html) are written in JavaScript and provide the ability to execute SQL through a JavaScript API. This makes it possible to leverage JavaScript and introduce complex procedural and error-handling logic or create SQL statements dynamically.

## commit\_table

This stored procedure is being used in the commit steps of the standard modules to create the `_staged` tables. It updates a target table with the data from the source table, overwritting any matching rows (based on the join key) in the target table and inserts non-matching rows. As such, it can also be useful to update any custom production tables one has created. An example of this can be seen in the custom module SQL directory. 


**Arguments**
 - source schema. The schema of the source table
 - source table. The table to copy data from.
 - target schema. The schema of the target table
 - target table. The table to copy data into.
 - join key. The key to join the source table to the target.
 - partition key. The date partition key. This helps limit table scans during the delete/insert operation.
 - automigrate flag. If true then a) an empty target table will be created based on the source table and b) if the target table already exists, any columns missing from the target table will be added.


**Example call**

```
  CALL derived.commit_table('scratch',                      -- source schema
                            'mobile_screen_views_this_run', -- source table
                            'scratch',                      -- target schema
                            'mobile_screen_views_staged',   -- target table
                            'screen_view_id',               -- join key
                            'derived_tstamp',               -- partition key
                             FALSE);                        -- automigrate
```
