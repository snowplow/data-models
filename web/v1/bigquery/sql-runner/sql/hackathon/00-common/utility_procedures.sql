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
