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

-- Function to count mismatched columns between source and target tables:
CREATE OR REPLACE FUNCTION {{.output_schema}}.columnCheckQuery (sourceDataset STRING,
                                                                sourceTable STRING,
                                                                targetDataset STRING,
                                                                targetTable STRING)
AS(
  (SELECT CONCAT("""SELECT
      SUM(CASE WHEN a.column_name IS NULL THEN 1 ELSE 0 END) AS missing_in_source,
      SUM(CASE WHEN b.column_name IS NULL THEN 1 ELSE 0 END) AS missing_in_target

    FROM
      (SELECT column_name, data_type, ordinal_position FROM """, sourceDataset,
    """.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '""", sourceTable,
    """') a
    FULL JOIN
      (SELECT column_name, data_type, ordinal_position FROM """, targetDataset,
    """.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '""", targetTable,
    """') b
    ON
      a.column_name = b.column_name
      AND a.ordinal_position = b.ordinal_position
  """)
  )
);

CREATE OR REPLACE PROCEDURE {{.output_schema}}.commit_table (sourceDataset STRING,
                                                             sourceTable STRING,
                                                             targetDataset STRING,
                                                             targetTable STRING,
                                                             joinKey STRING,
                                                             partitionKey STRING,
                                                             automigrate BOOLEAN)
BEGIN
  DECLARE COLS_NOT_IN_SOURCE, COLS_NOT_IN_TARGET INT64;
  DECLARE SOURCE_PATH, TARGET_PATH, DELETE_QUERY STRING;
  DECLARE COLUMN_ADDITIONS ARRAY<STRING>;
  DECLARE LOWER_LIMIT TIMESTAMP;

  SET (SOURCE_PATH, TARGET_PATH) = (CONCAT(sourceDataset, '.', sourceTable), CONCAT(targetDataset, '.', targetTable));

  IF automigrate THEN

    EXECUTE IMMEDIATE
      format("""CREATE TABLE IF NOT EXISTS %s
        PARTITION BY DATE(%s)
      AS (SELECT * FROM %s WHERE FALSE);""", TARGET_PATH, partitionKey, SOURCE_PATH);

  END IF;

  -- Check if any columns are missing from either source or target table
  EXECUTE IMMEDIATE {{.output_schema}}.columnCheckQuery(sourceDataset, sourceTable, targetDataset, targetTable) INTO COLS_NOT_IN_SOURCE, COLS_NOT_IN_TARGET;

  -- If source is missing a column, throw.
  IF COLS_NOT_IN_SOURCE > 0 THEN
    RAISE USING MESSAGE = 'ERROR: Source table is missing column(s) which exist in target table.';

  ELSEIF COLS_NOT_IN_TARGET > 0 AND NOT automigrate THEN
    RAISE USING MESSAGE = 'ERROR: Target table is missing column(s), but automigrate is disabled.';

  -- If target is missing a column, and automigrate is switched on, add the columns
  ELSEIF COLS_NOT_IN_TARGET > 0 AND automigrate THEN

    -- Query information schema to produce an ordered array of strings for columns and their types.
    EXECUTE IMMEDIATE
      format("""
      WITH columns AS(SELECT
        CONCAT(a.column_name, ' ', a.data_type) AS col_with_type,
        a.ordinal_position

      FROM
        (SELECT column_name, data_type, ordinal_position FROM %s.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '%s') a
      LEFT JOIN
        (SELECT column_name, data_type, ordinal_position FROM %s.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '%s') b
      ON
        a.column_name = b.column_name
        AND a.ordinal_position = b.ordinal_position

      WHERE b.column_name IS NULL)

      SELECT ARRAY(SELECT col_with_type FROM columns ORDER BY ordinal_position);
      """, sourceDataset, sourceTable, targetDataset, targetTable) INTO COLUMN_ADDITIONS;

    --- Execute add column statements
    EXECUTE IMMEDIATE format(
      """ALTER TABLE %s
        ADD COLUMN IF NOT EXISTS %s""", TARGET_PATH, ARRAY_TO_STRING(COLUMN_ADDITIONS, ', ADD COLUMN IF NOT EXISTS '));

  END IF;

  -- Get lower limit
  EXECUTE IMMEDIATE
    format("SELECT TIMESTAMP_SUB(MIN(%s), INTERVAL {{or .upsert_lookback_days 30}} DAY) FROM %s", partitionKey, TARGET_PATH) INTO LOWER_LIMIT;

  -- Perform DELETE <> INSERT transaction
  BEGIN

    -- Weird way to do it but table names can't go in 'USING' variables, but the CONCAT is v messy with duplicated 'Key' variable

    -- TODO: See if there's a cleaner way to go about this
    SET DELETE_QUERY = CONCAT("""DELETE FROM """, TARGET_PATH, """ WHERE """, joinKey, """ IN (SELECT """, joinKey, """ FROM """, SOURCE_PATH,
                              """) AND """, partitionKey, """ >= @LowerLimit;""");

    EXECUTE IMMEDIATE
      DELETE_QUERY
      USING LOWER_LIMIT AS LowerLimit;

    EXECUTE IMMEDIATE
      format("""INSERT %s (SELECT * FROM %s);""", TARGET_PATH, SOURCE_PATH);

  END;
END;

-- Extracts first element of a context array or an unstructured event struct. 
-- Returns ARRAY of STRUCTs mapping fields to their paths. The path is a COALSCE across columns versions, ordered by column version
-- e.g. COALESCE(contexts_com_snowplowanalytics_snowplow_mobile_context_1_0_1[SAFE_OFFSET(0)].device_model, contexts_com_snowplowanalytics_snowplow_mobile_context_1_0_0[SAFE_OFFSET(0)].device_model, NULL)
-- Currently only works if field names aren't duplicated, and all fields are top-level (ie no arrays and structs atm)

CREATE OR REPLACE PROCEDURE {{.output_schema}}.combine_field_versions (source_schema STRING,
                                                                       source_table STRING,
                                                                       source_fields STRING,       -- Array of fields to select. STRING to allow for concat. Use quoted array i.e. '["id"]'
                                                                       columns_prefix STRING,      -- Prefix of columns to concat across versions
                                                                       rename_fields_yn BOOLEAN,   -- Option to rename fields 
                                                                       renamed_fields STRING,      -- Array of names to rename fields to. STRING to allow for concat. Use quoted array i.e. '["session_id"]'
                                                                       OUT FIELDS ARRAY<STRUCT<field_name STRING, paths_coalesce STRING>>) -- Returns ARRAY of STRUCTs, mapping fields to paths.

BEGIN

  DECLARE SELECTOR, FIELD_VERSIONS_QUERY STRING;
  DECLARE NUM_SOURCE_FIELDS, NUM_RENAMED_FIELDS INT64;

  --If renaming fields, check the array lengths are the same between source_fields and renamed_fields
  IF rename_fields_yn THEN

    SET NUM_SOURCE_FIELDS = (ARRAY_LENGTH(ARRAY(SELECT * FROM UNNEST(SPLIT(SUBSTR(source_fields, 2 , LENGTH(source_fields) - 2))))));
    SET NUM_RENAMED_FIELDS = (ARRAY_LENGTH(ARRAY(SELECT * FROM UNNEST(SPLIT(SUBSTR(renamed_fields, 2 , LENGTH(renamed_fields) - 2))))));

    IF NUM_SOURCE_FIELDS != NUM_RENAMED_FIELDS THEN
      RAISE USING MESSAGE = 'ERROR: Source field and renamed field arrays are not the same length';
    END IF;

  END IF;

  -- Determines DTYPE of column and therefore suitable method to select fields from column
  SET SELECTOR = (
    SELECT 
      CASE WHEN columns_prefix LIKE 'contexts%' THEN "'[SAFE_OFFSET(0)].'"
           WHEN columns_prefix LIKE 'unstruct%' THEN "'.'" END
    );

  IF SELECTOR IS NULL THEN
    RAISE USING MESSAGE = 'ERROR: Unrecognized column type';
  END IF;

  SET FIELD_VERSIONS_QUERY = CONCAT(
    """WITH field_mapping AS (
      SELECT
        source_field,
        IF(""",rename_fields_yn,""", """,renamed_fields,"""[OFFSET(source_field_offset)], source_field) AS target_field
      
      FROM UNNEST(""",source_fields,""") AS source_field WITH OFFSET AS source_field_offset
    )

    , column_field_paths AS (
      SELECT
        field_path,
        column_name

      FROM """,source_schema,""".INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
      WHERE table_name = '""",source_table,"""'
      AND column_name LIKE CONCAT('""",columns_prefix,"""', '%')
      AND ARRAY_LENGTH(SPLIT(field_path, '.')) = 2
      AND SPLIT(field_path, '.')[SAFE_OFFSET(1)] IN (SELECT * FROM UNNEST(""",source_fields,"""))
      AND data_type NOT LIKE 'STRUCT%'
      AND data_type NOT LIKE 'ARRAY%'
    )

    , fields AS (
      SELECT
        SPLIT(field_path, '.')[SAFE_OFFSET(1)] AS field_name,
        ARRAY_AGG(CONCAT(column_name,""", SELECTOR, """, SPLIT(field_path, '.')[SAFE_OFFSET(1)]) ORDER BY column_name DESC) AS paths

      FROM column_field_paths
      GROUP BY 1
    )

    , renamed_fields AS (
      SELECT
        fm.target_field AS field_name,
        CONCAT('COALESCE(', ARRAY_TO_STRING(f.paths, ', '), ', NULL) ') AS paths_coalesce
      
      FROM field_mapping fm -- using mapping as spine. This ensure the order of the output is the same as the input.
      LEFT JOIN fields f
      ON fm.source_field = f.field_name
      WHERE f.paths IS NOT NULL -- ignore inputted field if no path present
    )

    SELECT ARRAY_AGG(STRUCT(field_name, paths_coalesce)) AS fields 
    
    FROM renamed_fields

    """);

  EXECUTE IMMEDIATE FIELD_VERSIONS_QUERY INTO FIELDS;

END;

-- For every STRUCT in the input ARRAY, concats path to the field name to generate a column name. All columns are then concat together into a comma separated string.
CREATE OR REPLACE PROCEDURE {{.output_schema}}.concat_fields (FIELDS ARRAY<STRUCT<field_name STRING, paths_coalesce STRING>>,
                                                              OUT FIELDS_CONCAT STRING)

BEGIN
  
  SET FIELDS_CONCAT = (SELECT ARRAY_TO_STRING(ARRAY_AGG(CONCAT(paths_coalesce, ' AS ', field_name)), ',') FROM UNNEST(FIELDS));

END;

-- Used to dynamically select the correct mobile context fields from events table.
-- MOBILE_CONTEXT_COLUMNS = Coalesce of fields within context e.g. "COALESCE(contexts_com_snowplowanalytics_snowplow_mobile_context_1_0_1[SAFE_OFFSET(0)].device_manufacturer, contexts_com_snowplowanalytics_snowplow_mobile_context_1_0_0[SAFE_OFFSET(0)].device_manufacturer) AS device_manufacturer"
CREATE OR REPLACE PROCEDURE {{.output_schema}}.mobile_mobile_context_fields (context_enabled BOOLEAN,
                                                                             OUT MOBILE_CONTEXT_COLUMNS STRING)

BEGIN

  DECLARE MOBILE_FIELDS ARRAY<STRUCT<field_name STRING, paths_coalesce STRING>>;
  --If mobile context enabled, find all fields across context schema versions, else return NULL fields.
  IF context_enabled THEN

    CALL {{.output_schema}}.combine_field_versions(
                    '{{.input_schema}}',     -- source_schema
                    'events',                -- source_table
                    '["device_manufacturer","device_model","os_type","os_version","android_idfa","apple_idfa","apple_idfv","carrier","open_idfa","network_technology","network_type"]', --source_fields. Quoted array to allow for concat
                    'contexts_com_snowplowanalytics_snowplow_mobile_context_1_0', -- columns_prefix
                    false,                   -- rename_y_n
                    '[""]',                  -- rename_fields
                    MOBILE_FIELDS            -- returns all fields in context + path
                    );

    CALL {{.output_schema}}.concat_fields(
                    MOBILE_FIELDS,
                    MOBILE_CONTEXT_COLUMNS   -- returned context coalesce columns
                    );
  ELSE

    SET MOBILE_CONTEXT_COLUMNS = (
      """CAST(NULL AS STRING) AS device_manufacturer,
      CAST(NULL AS STRING) AS device_model,
      CAST(NULL AS STRING) AS os_type,
      CAST(NULL AS STRING) AS os_version,
      CAST(NULL AS STRING) AS android_idfa,
      CAST(NULL AS STRING) AS apple_idfa,
      CAST(NULL AS STRING) AS apple_idfv,
      CAST(NULL AS STRING) AS carrier,
      CAST(NULL AS STRING) AS open_idfa,
      CAST(NULL AS STRING) AS network_technology,
      CAST(NULL AS STRING) AS network_type""");

  END IF;

END;


CREATE OR REPLACE PROCEDURE {{.output_schema}}.mobile_app_errors_fields (OUT APP_ERRORS_EVENTS_COLUMNS STRING)

BEGIN

  DECLARE APP_ERROR_FIELDS ARRAY<STRUCT<field_name STRING, paths_coalesce STRING>>;

  CALL {{.output_schema}}.combine_field_versions(
                  '{{.scratch_schema}}',      -- source_schema
                  'mobile_events_staged',     -- source_table
                  '["message","programming_language","class_name","exception_name","is_fatal","line_number","stack_trace","thread_id","thread_name"]', --source_fields. Quoted array to allow for concat
                  'unstruct_event_com_snowplowanalytics_snowplow_application_error_1_0', -- columns_prefix
                  false,                      -- rename_y_n
                  '[""]',                     -- rename_fields
                  APP_ERROR_FIELDS            -- returns all fields in event + path 
                  );
  
  CALL {{.output_schema}}.concat_fields(
                    APP_ERROR_FIELDS,
                    APP_ERRORS_EVENTS_COLUMNS -- returned event coalesce columns
                    );

END;


CREATE OR REPLACE PROCEDURE {{.output_schema}}.mobile_session_context_fields (OUT SESSION_ID STRING,
                                                                              OUT SESSION_CONTEXT_COLUMNS STRING)

BEGIN

  DECLARE SESSION_FIELDS ARRAY<STRUCT<field_name STRING, paths_coalesce STRING>>;
  --Mandatory context. Returns all fields across session context schema versions.
  CALL {{.output_schema}}.combine_field_versions(
                  '{{.input_schema}}',      -- source_schema
                  'events',                 -- source_table
                  '["session_id","session_index","previous_session_id","user_id","first_event_id"]', --source_fields. Quoted array to allow for concat
                  'contexts_com_snowplowanalytics_snowplow_client_session_1_0', -- columns_prefix
                  true,                     -- rename_y_n
                  '["session_id","session_index","previous_session_id","device_user_id","session_first_event_id"]', -- rename_fields
                  SESSION_FIELDS            -- returns all fields in context + path
                  );

  CALL {{.output_schema}}.concat_fields(
                    SESSION_FIELDS,
                    SESSION_CONTEXT_COLUMNS -- returned context coalesce columns
                    );

  SET SESSION_ID = (
    SELECT paths_coalesce FROM UNNEST(SESSION_FIELDS) WHERE field_name = 'session_id'
    );

END;
