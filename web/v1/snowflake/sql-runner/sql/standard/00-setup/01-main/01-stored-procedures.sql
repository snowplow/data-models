/*
   Copyright 2021-2022 Snowplow Analytics Ltd. All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/


/*
  MK_TRANSACTION
  Side effects procedure.
  Input: a concatenation of one or more DML sql statements split by semicolon.
  It is important that the statements are not DDL.
  Either all the statements in the block succeed and get committed or all get rolled back.
  To drop:
  DROP PROCEDURE {{.output_schema}}.mk_transaction(VARCHAR);
*/
CREATE OR REPLACE PROCEDURE {{.output_schema}}.mk_transaction(DML_STATEMENTS VARCHAR)
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
  AS
  $$

  dmls = DML_STATEMENTS.split(';')
                       .filter(function(stmt) {return stmt.trim() !== '';})
                       .map(function(stmt) {return stmt.trim() + ';';});

  snowflake.createStatement({sqlText: `BEGIN;`}).execute();
  try {

      dmls.forEach(function(stmt) {
          snowflake.createStatement({sqlText: stmt}).execute();
      });
      snowflake.createStatement({sqlText: `COMMIT;`}).execute();

  } catch(ERROR) {
      snowflake.createStatement({sqlText: `ROLLBACK;`}).execute();

      // Snowflake error is not very helpful here
      var err_msg = "Transaction rolled back.Probably failed: " + DML_STATEMENTS + " :Error: ";
      throw Error(err_msg + ERROR);
  }

  return "ok. Statements in transaction succeeded."

  $$
;


/*
  COLUMN_CHECK
  Checks for mismatched columns between source and target tables.
  If source table is missing columns, it errors.
  If AUTOMIGRATE is 'TRUE',
    it allows the target table to get added the additional columns,if any, of source.
    Note: Only if extra columns of source are after the common ones, will get added.
  Since ALTER TABLE is a DDL statement, it will be its own transaction.
  This means that even if it was placed inside another transaction,
    it would commit that, and then start another(implicit) for its own execution.
  So, also it cannot be explicitly rolled back.
  Input:
  SOURCE_SCHEMA:  the schema of the source table
  SOURCE_TABLE:   the source table name
  TARGET_SCHEMA:  the schema of the target table
  TARGET_TABLE:   the target table name
  AUTOMIGRATE:    whether target tabled gets altered if needed (only 'TRUE' enables)
  To drop:
  DROP PROCEDURE {{.output_schema}}.column_check(VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR);
*/
CREATE OR REPLACE PROCEDURE {{.output_schema}}.column_check(SOURCE_SCHEMA VARCHAR,
                                                            SOURCE_TABLE  VARCHAR,
                                                            TARGET_SCHEMA VARCHAR,
                                                            TARGET_TABLE  VARCHAR,
                                                            AUTOMIGRATE   VARCHAR)
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
  AS
  $$

  column_check_stmt = `
    WITH target_columns AS (
    SELECT
      isc.column_name,
      isc.data_type,
      isc.ordinal_position,
      isc.character_maximum_length

    FROM information_schema.columns AS isc
    WHERE table_schema = UPPER(:1)
    AND table_name = UPPER(:2)
    )

  , source_columns AS (
    SELECT
      isc.column_name,
      isc.data_type,
      isc.ordinal_position,
      isc.character_maximum_length,
      isc.numeric_precision,
      isc.numeric_scale

    FROM information_schema.columns AS isc
    WHERE table_schema = UPPER(:3)
    AND table_name = UPPER(:4)
    )

  SELECT
    SUM(CASE WHEN sc.column_name IS NULL THEN 1 ELSE 0 END) AS missing_in_source,
    SUM(CASE WHEN tc.column_name IS NULL THEN 1 ELSE 0 END) AS missing_in_target,
    LISTAGG(
      CASE
      WHEN tc.column_name IS NOT NULL
        THEN NULL
      WHEN sc.data_type='TEXT'
        THEN CONCAT(sc.column_name, ' VARCHAR(',sc.character_maximum_length, ')')
      WHEN sc.data_type='NUMBER'
        THEN CONCAT(sc.column_name, ' NUMBER(', sc.numeric_precision, ',',sc.numeric_scale, ')')
      ELSE
        CONCAT(sc.column_name, ' ', sc.data_type)
      END
      , ', ') WITHIN GROUP (ORDER BY sc.ordinal_position) AS cols_to_add,
      LISTAGG(CASE WHEN tc.column_name IS NOT NULL AND sc.character_maximum_length > tc.character_maximum_length THEN sc.column_name END, ', ') as cols_w_incompatible_char_limits

  FROM target_columns tc
  FULL OUTER JOIN source_columns sc
  ON tc.column_name = sc.column_name
  AND tc.data_type = sc.data_type
  AND tc.ordinal_position = sc.ordinal_position`;

  var res = snowflake.createStatement({sqlText: column_check_stmt,
                                       binds: [TARGET_SCHEMA, TARGET_TABLE,SOURCE_SCHEMA, SOURCE_TABLE]}
                                      ).execute();
  res.next();

  missing_in_source = res.getColumnValue(1);
  missing_in_target = res.getColumnValue(2);
  cols_to_add = res.getColumnValue(3);
  cols_with_varchar_issue = res.getColumnValue(4);


  if (missing_in_source > 0) {
    throw "ERROR: Source table is either missing column(s) which exist in target table or their position is wrong.";
  }

  if (cols_with_varchar_issue !== '') {
    throw "ERROR: field length for source varchar column(s) " + cols_with_varchar_issue + " is longer than the target."
  }

  if (missing_in_target > 0) {

    if ( AUTOMIGRATE !== 'TRUE' ) {
      throw "ERROR: Target table is missing column(s),but automigrate is not enabled.";

    } else {
        var alter_stmt = `ALTER TABLE ` + TARGET_SCHEMA + `.` + TARGET_TABLE + ` ADD COLUMN ` + cols_to_add;
        snowflake.createStatement({sqlText: alter_stmt}).execute();
        return "ok. Columns added."
      }

  } else {
        return "ok. Columns match."
    }

  $$
;


/*
  COMMIT_STAGED
  Inputs:
  AUTOMIGRATE:           whether to automigrate staged table
  STAGING_SOURCE:        the staging source table
  STAGING_TARGET:        the staging target table
  STAGING_JOIN_KEY:      the join key for staged tables
  To drop:
  DROP PROCEDURE {{.output_schema}}.commit_staged(VARCHAR,VARCHAR,VARCHAR,VARCHAR);
*/
CREATE OR REPLACE PROCEDURE {{.output_schema}}.commit_staged(
                                                 AUTOMIGRATE           VARCHAR,
                                                 STAGING_SOURCE        VARCHAR,
                                                 STAGING_TARGET        VARCHAR,
                                                 STAGING_JOIN_KEY      VARCHAR)
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
  AS
  $$

  stg_target_split = STAGING_TARGET.split('.');
  stg_source_split = STAGING_SOURCE.split('.');

  if (AUTOMIGRATE === 'TRUE') {
      var cols_check = `CALL {{.output_schema}}.column_check(
                            '` + stg_source_split[0] + `'
                          , '` + stg_source_split[1] + `'
                          , '` + stg_target_split[0] + `'
                          , '` + stg_target_split[1] + `'
                          , '` + AUTOMIGRATE + `');`;
      snowflake.createStatement({sqlText: cols_check}).execute();
  }
  var stg_trg_columns = list_cols(stg_target_split[0],stg_target_split[1]);
  var stg_del_condition = `` + STAGING_JOIN_KEY + ` IN
                        (SELECT ` + STAGING_JOIN_KEY + ` FROM ` + STAGING_SOURCE + `)`;

  var stg_delete_stmt = `
      DELETE FROM ` + STAGING_TARGET + `
      WHERE ` + stg_del_condition + `;`;

  var stg_insert_stmt = `
      INSERT INTO ` + STAGING_TARGET + `
        SELECT ` + stg_trg_columns + `
        FROM ` + STAGING_SOURCE + `;`;

  // BEGIN TRANSACTION
  snowflake.createStatement({sqlText: `BEGIN;`}).execute();
  try {

      snowflake.createStatement({sqlText: stg_delete_stmt}).execute();
      snowflake.createStatement({sqlText: stg_insert_stmt}).execute();
      snowflake.createStatement({sqlText: `COMMIT;`}).execute();

  } catch(ERROR) {

      snowflake.createStatement({sqlText: `ROLLBACK;`}).execute();
      throw ERROR;

  }
  return "ok. commit_staged succeeded.";

  // == Helpers ==
  function list_cols(sch,tbl) {
      var stmt = `
          SELECT listagg(isc.column_name, ',') WITHIN GROUP (order by isc.ordinal_position)
          FROM information_schema.columns AS isc
          WHERE table_schema='` + sch + `'
            AND table_name='` + tbl + `';`;

      var res = snowflake.createStatement({sqlText: stmt}).execute();
      res.next();
      result = res.getColumnValue(1);

      return result;
  }

  $$
;
