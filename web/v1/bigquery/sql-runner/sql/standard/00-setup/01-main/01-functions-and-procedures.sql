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
