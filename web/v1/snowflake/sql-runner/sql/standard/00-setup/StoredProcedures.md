# Stored procedures

Below you can read about stored procedured defined for the Snowflake modules. They are defined under the `{{.output_schema}}` and you can find the source code [here](./01-main/01-stored-procedures.sql).

For modularity, they are recreated in the first step of all `01-main` playbooks and they are explicitly dropped in the `XX-destroy` steps.

Snowflake's [stored procedures](https://docs.snowflake.com/en/sql-reference/stored-procedures.html) are written in JavaScript and provide the ability to execute SQL through a JavaScript API. This makes it possible to leverage JavaScript and introduce complex procedural and error-handling logic or create SQL statements dynamically.


## mk\_transaction

This is a stored procedure created to group DML statements into an atomic transaction. It mainly addresses what Snowflake docs describe [here](https://docs.snowflake.com/en/sql-reference/transactions.html#failed-statements-within-a-transaction) as:

> Although a transaction is committed or rolled back as a unit, that is not quite the same as saying that it succeeds or fails as a unit. If a statement fails within a transaction, you can still commit, rather than roll back, the transaction.


The `mk_transaction` procedure can be used when you want to ensure that either all the statements in the block succeed and get committed or all get rolled back.

**Argument**

 - A concatenation of DML statements separated by semicolon.

**Notes**

1. It is important that the statements are not DDL. DDL statements (e.g. `CREATE TABLE`) execute on their own transaction, so including them essentially "breaks" the abortability.
2. The DML statements inside the string argument are expected to be separated by semicolon and comments are not handled.

**Example call**

```
CALL derived.mk_transaction(
 '
 DELETE FROM A_TBL;
 INSERT INTO A_TBL VALUES (1,2),(3,4);
 TRUNCATE TABLE B_TBL;
 '
);
```

## commit\_staged

This stored procedure is being used in the commit steps of the standard modules to create the `_staged` tables, if `stage_next` is true.


**Arguments**

 - staging automigrate flag
 - staging source table
 - staging target table
 - staging join key

**Example call**

```
CALL commit_staged('TRUE',
                   UPPER('scratch.page_views_this_run'),
                   UPPER('scratch.page_views_staged'),
                   UPPER('page_view_id'));
```

## Troubleshooting

When troubleshooting, you can also consider:
 - The case sensitivity of Snowflake (you will notice that in our calls we use `UPPER` for varchar arguments). You can read more about it [here](https://docs.snowflake.com/en/sql-reference/stored-procedures-usage.html#case-sensitivity-in-javascript-arguments).
 - The flags are of `VARCHAR` type and not of `BOOLEAN`.
 - Besides the order, the number of arguments matters. Stored procedure names can be overloaded.

Also, feel free to reach out!
