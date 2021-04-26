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

-- A table storing an identifier for this run of a model - used to identify runs of the model across multiple modules/steps (eg. base, page views share this id per run)
CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.{{.model}}_metadata_run_id{{.entropy}} (
    run_id TIMESTAMP
);

-- When base runs, it's always the first module. So it's safe to just truncate here.
TRUNCATE TABLE {{.scratch_schema}}.{{.model}}_metadata_run_id{{.entropy}};

INSERT INTO {{.scratch_schema}}.{{.model}}_metadata_run_id{{.entropy}} (
  SELECT
    CURRENT_TIMESTAMP()
);

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

-- Setup temp metadata tables for this run
CREATE OR REPLACE TABLE {{.scratch_schema}}.{{.model}}_base_metadata_this_run{{.entropy}} (
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
);

INSERT INTO {{.scratch_schema}}.{{.model}}_base_metadata_this_run{{.entropy}} (
  SELECT
    'run',
    run_id,
    '{{.model_version}}',
    '{{.model}}',
    'base',
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
  FROM {{.scratch_schema}}.{{.model}}_metadata_run_id{{.entropy}}
);


-- Setup manifests
CREATE TABLE IF NOT EXISTS {{.output_schema}}.{{.model}}_base_event_id_manifest{{.entropy}}
PARTITION BY DATE(collector_tstamp)
AS (
  SELECT
    'seed' AS event_id,
    TIMESTAMP('{{.start_date}}') AS collector_tstamp
);

CREATE TABLE IF NOT EXISTS {{.output_schema}}.{{.model}}_base_session_id_manifest{{.entropy}}
PARTITION BY DATE(min_tstamp)
AS (
  SELECT
    'seed' AS session_id,
    TIMESTAMP('{{.start_date}}') AS min_tstamp
);
