/*
   Copyright 2020-2021 Snowplow Analytics Ltd. All rights reserved.

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

-- Extracts first element of context array, and coalesces the fields across versions.
-- Currently only works if field names aren't duplicated, and all fields are top-level (ie no arrays and structs atm)

CREATE OR REPLACE PROCEDURE {{.output_schema}}.combine_context_versions (columns_prefix STRING)
BEGIN
  DECLARE COLUMN_COALESCE STRING;

  SET COLUMN_COALESCE = (
    -- Flatten results of nested query into comma separated list of coalesces
    SELECT ARRAY_TO_STRING(
      ARRAY_AGG(CONCAT('COALESCE(', ARRAY_TO_STRING(paths, ', '), ', NULL) AS ',  field_name)), ', ')
    FROM(
      -- Get field names and their corresponding paths
      SELECT
        SPLIT(field_path, '.')[SAFE_OFFSET(1)] AS field_name,
        ARRAY_AGG(CONCAT(column_name, '[SAFE_OFFSET(0)].', SPLIT(field_path, '.')[SAFE_OFFSET(1)]) ORDER BY column_name DESC) AS paths

      FROM {{.scratch_schema}}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
      WHERE table_name = 'events_staged{{.entropy}}'
      AND column_name LIKE CONCAT(columns_prefix, '%')
      AND ARRAY_LENGTH(SPLIT(field_path, '.')) = 2 -- Only first-order fields
      AND data_type NOT LIKE 'STRUCT%' -- No structs
      AND data_type NOT LIKE 'ARRAY%' -- No arrays
      GROUP BY 1
    )
  );

  IF COLUMN_COALESCE IS NULL THEN
    RAISE USING MESSAGE = 'ERROR: Cannot combine context versions: No eligible top-level columns found.';

  END IF;

  -- Create scratch table with extracted data:
  EXECUTE IMMEDIATE CONCAT("""CREATE OR REPLACE TABLE {{.scratch_schema}}.""",
                            columns_prefix,
                            """{{.entropy}} AS( SELECT event_id, page_view_id, collector_tstamp, derived_tstamp, """,
                            COLUMN_COALESCE,
                            """ FROM {{.scratch_schema}}.events_staged{{.entropy}})""");
END;
