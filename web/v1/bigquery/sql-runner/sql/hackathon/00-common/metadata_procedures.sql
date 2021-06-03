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


CREATE OR REPLACE PROCEDURE {{.scratch_schema}}.metadata_setup ()
BEGIN
  DECLARE RUN_ID TIMESTAMP;

  -- A table storing an identifier for this run of a model - used to identify runs of the model across multiple modules/steps (eg. base, page views share this id per run)
  CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.metadata_run_id{{.entropy}} (
      run_id TIMESTAMP
  );

  -- Insert new run_id if one doesn't exist
  SET RUN_ID = (SELECT run_id FROM {{.scratch_schema}}.metadata_run_id{{.entropy}} LIMIT 1);

  IF RUN_ID IS NULL THEN
    INSERT INTO {{.scratch_schema}}.metadata_run_id{{.entropy}} (
      SELECT
        CURRENT_TIMESTAMP()
    );
  END IF;

  -- Permanent metadata table
  CREATE TABLE IF NOT EXISTS {{.output_schema}}.datamodel_metadata{{.entropy}} (
    run_id TIMESTAMP,
    model_version STRING,
    model STRING,
    module STRING,
    run_start_tstamp TIMESTAMP,
    run_end_tstamp TIMESTAMP,
    rows_this_run INT64,
    distinct_key STRING,
    distinct_key_count INT64,
    time_key STRING,
    min_time_key TIMESTAMP,
    max_time_key TIMESTAMP,
    duplicate_rows_removed INT64,
    distinct_keys_removed INT64
  )
  PARTITION BY DATE(run_start_tstamp);
  
  CALL {{.scratch_schema}}.log_model_table('{{.output_schema}}.datamodel_metadata{{.entropy}}', 'prod', 'metadata');
  CALL {{.scratch_schema}}.log_model_table('{{.scratch_schema}}.metadata_run_id{{.entropy}}', 'prod', 'metadata');
  -- metadata run id is a temp table but should only be cleaned up by the ends_run flag, so we mark as prod level.
  -- It is logged, however, since it should be removed when we destroy everything.
END;

-- Better name for this function?
CREATE OR REPLACE PROCEDURE {{.scratch_schema}}.init_metadata_logging(step_name STRING)
BEGIN
  DECLARE STEP_METADATA_TABLE_PATH, STEP_METADATA_CREATE, STEP_METADATA_INSERT STRING;
  
  SET STEP_METADATA_TABLE_PATH = CONCAT('{{.scratch_schema}}.metadata_this_run_', step_name, '{{.entropy}}');

  SET STEP_METADATA_CREATE = CONCAT("""
  CREATE OR REPLACE TABLE """, STEP_METADATA_TABLE_PATH, """ (
    id STRING,
    run_id TIMESTAMP,
    model_version STRING,
    model STRING,
    module STRING,
    run_start_tstamp TIMESTAMP,
    run_end_tstamp TIMESTAMP,
    rows_this_run INT64,
    distinct_key STRING,
    distinct_key_count INT64,
    time_key STRING,
    min_time_key TIMESTAMP,
    max_time_key TIMESTAMP,
    duplicate_rows_removed INT64,
    distinct_keys_removed INT64
  );""");

  SET STEP_METADATA_INSERT = CONCAT("""
  INSERT INTO """, STEP_METADATA_TABLE_PATH, """ (
    SELECT
      'run',
      run_id,
      '2.0.0-hackathon.test',
      'web',
      '""", step_name, """',
      CURRENT_TIMESTAMP(),
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL

    FROM {{.scratch_schema}}.metadata_run_id{{.entropy}}
  );
  """);
  
  EXECUTE IMMEDIATE STEP_METADATA_CREATE;
  EXECUTE IMMEDIATE STEP_METADATA_INSERT;
  
  CALL {{.scratch_schema}}.log_model_table(STEP_METADATA_TABLE_PATH, 'trace', step_name);
END;


CREATE OR REPLACE PROCEDURE {{.scratch_schema}}.stage_step_metadata (step_name STRING, distinct_key STRING, time_key STRING, input_table_path STRING)
OPTIONS(strict_mode=false)
BEGIN
  DECLARE TEMP_METADATA_TABLE_PATH, STEP_METADATA_TABLE_PATH, TEMP_METADATA_CREATE, STEP_METADATA_UPDATE STRING;
  
  SET TEMP_METADATA_TABLE_PATH = CONCAT('{{.scratch_schema}}.run_metadata_temp_', step_name, '{{.entropy}}');
  SET STEP_METADATA_TABLE_PATH = CONCAT('{{.scratch_schema}}.metadata_this_run_', step_name, '{{.entropy}}');

  SET TEMP_METADATA_CREATE = CONCAT("""
  CREATE OR REPLACE TABLE """, TEMP_METADATA_TABLE_PATH, """ AS (
    SELECT
      'run' AS id,
      count(*) AS rows_this_run,
      '""", distinct_key, """' AS distinct_key,
      count(DISTINCT """, distinct_key ,""") AS distinct_key_count,
      '""", time_key, """' AS time_key,
      MIN(""", time_key, """) AS min_time_key,
      MAX(""", time_key, """) AS max_time_key

    FROM 
      """, input_table_path, """
  );""");

  SET STEP_METADATA_UPDATE = CONCAT("""
  UPDATE """, STEP_METADATA_TABLE_PATH, """ a
    SET
      rows_this_run = b.rows_this_run,
      distinct_key = b.distinct_key,
      distinct_key_count = b.distinct_key_count,
      time_key = b.time_key,
      min_time_key = b.min_time_key,
      max_time_key = b.max_time_key

    FROM 
    """, TEMP_METADATA_TABLE_PATH, """ b
    WHERE a.id = b.id;""");
    
  EXECUTE IMMEDIATE TEMP_METADATA_CREATE;
  EXECUTE IMMEDIATE STEP_METADATA_UPDATE;

  CALL {{.scratch_schema}}.log_model_table(TEMP_METADATA_TABLE_PATH, 'trace', step_name);
END;

CREATE OR REPLACE PROCEDURE {{.scratch_schema}}.commit_step_metadata (step_name STRING)
BEGIN
  DECLARE STEP_METADATA_TABLE_PATH, METADATA_INSERT STRING;
  
  SET STEP_METADATA_TABLE_PATH = CONCAT('{{.scratch_schema}}.metadata_this_run_', step_name, '{{.entropy}}');

  SET METADATA_INSERT = CONCAT("""
  -- Commit metadata
  INSERT {{.output_schema}}.datamodel_metadata{{.entropy}} (
    SELECT
      run_id,
      model_version,
      model,
      module,
      run_start_tstamp,
      CURRENT_TIMESTAMP() AS run_end_tstamp,
      rows_this_run,
      distinct_key,
      distinct_key_count,
      time_key,
      min_time_key,
      max_time_key,
      duplicate_rows_removed,
      distinct_keys_removed
    FROM """, STEP_METADATA_TABLE_PATH, """);""");
END;
