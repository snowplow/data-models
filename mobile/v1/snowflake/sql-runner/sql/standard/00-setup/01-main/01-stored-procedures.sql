/*
   Copyright 2021 Snowplow Analytics Ltd. All rights reserved.

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
  UPDATE_MANIFEST
  Updates manifest in either the base or users module.
  Procedure required in order to rollback all DML statements if one fails within the transaction.
  Inputs:
  MODULE:   Name of the module containing the manifests to update.
  To drop:
  DROP PROCEDURE {{.output_schema}}.update_manifest(VARCHAR);
*/
CREATE OR REPLACE PROCEDURE {{.output_schema}}.update_manifest(MODULE VARCHAR)
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
  AS
  $$

  if (MODULE === 'base') {
      var event_manifest_delete = `
        DELETE FROM {{.output_schema}}.{{.model}}_base_event_id_manifest{{.entropy}}
          WHERE
            event_id IN (SELECT event_id FROM {{.scratch_schema}}.{{.model}}_events_this_run{{.entropy}})
          AND
            collector_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.{{.model}}_base_run_limits{{.entropy}});`;

      var event_manifest_insert = `
        INSERT INTO {{.output_schema}}.{{.model}}_base_event_id_manifest{{.entropy}}
          SELECT event_id, collector_tstamp FROM {{.scratch_schema}}.{{.model}}_events_this_run{{.entropy}};`;

      var session_manifest_delete = `
        DELETE FROM {{.output_schema}}.{{.model}}_base_session_id_manifest{{.entropy}}
          WHERE
            session_id IN (SELECT session_id FROM {{.scratch_schema}}.{{.model}}_base_sessions_to_include{{.entropy}});`;

      var session_manifest_insert = `
        INSERT INTO {{.output_schema}}.{{.model}}_base_session_id_manifest{{.entropy}}
          SELECT * FROM {{.scratch_schema}}.{{.model}}_base_sessions_to_include{{.entropy}};`;

      dmls = [event_manifest_delete, event_manifest_insert, session_manifest_delete, session_manifest_insert];
  }
  else if (MODULE === 'users') {
      var users_manifest_delete = `
        DELETE FROM {{.output_schema}}.mobile_users_manifest{{.entropy}}
        WHERE device_user_id IN (SELECT device_user_id FROM {{.scratch_schema}}.mobile_users_userids_this_run{{.entropy}});`;

      var users_manifest_insert = `
        INSERT INTO {{.output_schema}}.mobile_users_manifest{{.entropy}}
        SELECT * FROM {{.scratch_schema}}.mobile_users_userids_this_run{{.entropy}};`;

      var users_manifest_truncate = `
        TRUNCATE TABLE {{.scratch_schema}}.mobile_sessions_userid_manifest_staged{{.entropy}};`;

      dmls = [users_manifest_delete, users_manifest_insert, users_manifest_truncate];
  }
  else {
      throw "No manifest updated. Pass either 'base' or 'users' to update the manifests within that module";
  }

  snowflake.createStatement({sqlText: `BEGIN;`}).execute();
  try {

      dmls.forEach(function(stmt) {
          snowflake.createStatement({sqlText: stmt}).execute();
      });
      snowflake.createStatement({sqlText: `COMMIT;`}).execute();

  } catch(ERROR) {
      snowflake.createStatement({sqlText: `ROLLBACK;`}).execute();

      throw Error("Transaction rolled back. Error:" + ERROR);
  }

  return "Success. Manifests in the " + MODULE + " have been updated";

  $$
;


/*
  COLUMN_CHECKER
  Calculates how many columns in source table are present in the target table, and vice versa.
  Inputs:
  SOURCE_SCHEMA:         the schema of the source table
  SOURCE_TABLE:          the source table
  TARGET_SCHEMA:         the schema of the target table
  TARGET_TABLE:          the target table
  Output:
  column_results:        array. [missing_in_source, missing_in_target, columns_to_add]
  To drop:
  DROP PROCEDURE {{.output_schema}}.column_checker(VARCHAR,VARCHAR,VARCHAR,VARCHAR);
*/
CREATE OR REPLACE PROCEDURE {{.output_schema}}.column_checker(SOURCE_SCHEMA VARCHAR,
                                                              SOURCE_TABLE  VARCHAR,
                                                              TARGET_SCHEMA VARCHAR,
                                                              TARGET_TABLE  VARCHAR)
  RETURNS ARRAY
  LANGUAGE JAVASCRIPT
  AS
  $$

  column_check_stmt = `
  WITH target_columns AS (
    SELECT
      isc.column_name,
      isc.data_type,
      isc.ordinal_position

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
      , ',') WITHIN GROUP (ORDER BY sc.ordinal_position) AS cols_to_add

  FROM target_columns tc
  FULL OUTER JOIN source_columns sc
  ON tc.column_name = sc.column_name
  AND tc.data_type = sc.data_type
  AND tc.ordinal_position = sc.ordinal_position`;

  var res = snowflake.createStatement({sqlText: column_check_stmt,
                                       binds: [TARGET_SCHEMA, TARGET_TABLE, SOURCE_SCHEMA, SOURCE_TABLE]}
                                      ).execute();
  res.next();

  missing_in_source = res.getColumnValue(1);
  missing_in_target = res.getColumnValue(2);
  columns_to_add = res.getColumnValue(3);

  return [missing_in_source, missing_in_target, columns_to_add];

  $$
;


/*
  ALTER_TABLE
  Adds the specified columns to the target table.
  Inputs:
  TARGET_SCHEMA:         the schema of the target table
  TARGET_TABLE:          the target table to add columns to
  COLUMNS_TO_ADD:        comma seperated list of colum definitions to add e.g. event_name VARCHAR
  To drop:
  DROP PROCEDURE {{.output_schema}}.alter_table(VARCHAR,VARCHAR,VARCHAR);
*/
CREATE OR REPLACE PROCEDURE {{.output_schema}}.alter_table(TARGET_SCHEMA VARCHAR,
                                                           TARGET_TABLE  VARCHAR,
                                                           COLUMNS_TO_ADD VARCHAR)
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
  AS
  $$

  var target_namespace = TARGET_SCHEMA + `.` + TARGET_TABLE;

  var alter_table_stmt = `ALTER TABLE identifier(:1) ADD COLUMN ` + COLUMNS_TO_ADD + `;`;

  snowflake.createStatement({sqlText: alter_table_stmt,
                             binds: [target_namespace]}).execute();

  return "Success. Altered table: " + target_namespace + ". Added columns: " + COLUMNS_TO_ADD;

  $$
;


/*
  COMMIT_TABLE
  Deletes and inserts data from the source table to the target.
  If AUTOMIGRATE is enabled:
    a) The target table will be created if it does not currently exist. 
    b) If the target table already exists but does not contain all the columns in the source table, these columns will be added. 
  Inputs:
  SOURCE_SCHEMA:         the schema of the source table
  SOURCE_TABLE:          the source table to select data from
  TARGET_SCHEMA:         the schema of the target table
  TARGET_TABLE:          the target table to delete/insert into
  JOIN_KEY:              the join key between the source and target tables
  PARTITION_KEY:         the partition of the target table
  AUTOMIGRATE:           whether to automigrate staged table
  To drop:
  DROP PROCEDURE {{.output_schema}}.commit_table(VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,BOOLEAN);
*/
CREATE OR REPLACE PROCEDURE {{.output_schema}}.commit_table(SOURCE_SCHEMA VARCHAR,
                                                            SOURCE_TABLE  VARCHAR,
                                                            TARGET_SCHEMA VARCHAR,
                                                            TARGET_TABLE  VARCHAR,
                                                            JOIN_KEY      VARCHAR,
                                                            PARTITION_KEY VARCHAR,
                                                            AUTOMIGRATE   BOOLEAN)
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
  AS
  $$

  SOURCE_NAMESPACE = SOURCE_SCHEMA + '.' + SOURCE_TABLE;
  TARGET_NAMESPACE = TARGET_SCHEMA + '.' + TARGET_TABLE;

  if (AUTOMIGRATE) {
      /* Create table if doesnt already exists */
      var create_target_table_query = `CREATE TABLE IF NOT EXISTS identifier(:1) AS (SELECT * FROM identifier(:2) WHERE FALSE);`;
      snowflake.createStatement({sqlText: create_target_table_query,
                                 binds: [TARGET_NAMESPACE, SOURCE_NAMESPACE]}).execute();
  }

  /* Check cols between staging and target and add if required */
  var column_checker_procedure = `CALL {{.output_schema}}.column_checker(:1,:2,:3,:4);`;
  
  cols_checker_array = snowflake.createStatement({sqlText: column_checker_procedure,
                                                  binds: [SOURCE_SCHEMA, SOURCE_TABLE, TARGET_SCHEMA, TARGET_TABLE]}
                                                ).execute();
  cols_checker_array.next();

  [missing_in_source, missing_in_target, columns_to_add] = cols_checker_array.getColumnValue(1);

  if (missing_in_source > 0) {
      throw "ERROR: Source table is missing column(s) which exist in target table.";
  }
  else if (missing_in_target > 0 && !AUTOMIGRATE) {
      throw "ERROR: Target table is missing column(s), but automigrate is disabled.";
  }
  else if (missing_in_target > 0 && AUTOMIGRATE) {
      /* Alter table if AUTOMIGRATE enabled */
      var alter_table_procedure = `CALL {{.output_schema}}.alter_table(:1,:2,:3);`;

      snowflake.createStatement({sqlText: alter_table_procedure,
                                 binds: [TARGET_SCHEMA, TARGET_TABLE, columns_to_add]}
                                ).execute();
  }

  /* Prepare delete/insert statements */
  var trg_delete_stmt = `DELETE FROM identifier(:1) 
                         WHERE identifier(:2) IN (SELECT identifier(:2) FROM identifier(:3)) 
                         AND identifier(:4) >= (SELECT TIMEADD(DAY, -{{or .upsert_lookback_days 30}}, MIN(identifier(:4))) FROM identifier(:3));`;

  var tgr_insert_stmt = `INSERT INTO identifier(:1) SELECT * FROM identifier(:2);`;

  /* Execute delete/insert */
  snowflake.createStatement({sqlText: `BEGIN;`}).execute();
  try {

      snowflake.createStatement({sqlText: trg_delete_stmt,
                                binds: [TARGET_NAMESPACE, JOIN_KEY, SOURCE_NAMESPACE, PARTITION_KEY]}
                               ).execute();
      snowflake.createStatement({sqlText: tgr_insert_stmt,
                                binds: [TARGET_NAMESPACE, SOURCE_NAMESPACE]}
                              ).execute();
      snowflake.createStatement({sqlText: `COMMIT;`}).execute();

  } catch(ERROR) {

      snowflake.createStatement({sqlText: `ROLLBACK;`}).execute();
      throw ERROR;

  }

  return "Success. Inserted data from: " + SOURCE_NAMESPACE + " to: " + TARGET_NAMESPACE;

  $$
;
