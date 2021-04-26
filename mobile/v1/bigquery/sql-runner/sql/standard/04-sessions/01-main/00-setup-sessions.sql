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


DECLARE RUN_ID TIMESTAMP;

-- A table storing an identifier for this run of a model - used to identify runs of the model across multiple modules/steps (eg. base, page views share this id per run)
CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.mobile_metadata_run_id{{.entropy}} (
    run_id TIMESTAMP
);

-- Insert new run_id if one doesn't exist
SET RUN_ID = (SELECT run_id FROM {{.scratch_schema}}.mobile_metadata_run_id{{.entropy}} LIMIT 1);

IF RUN_ID IS NULL THEN
  INSERT INTO {{.scratch_schema}}.mobile_metadata_run_id{{.entropy}} (
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
);

-- Setup Metadata
CREATE OR REPLACE TABLE {{.scratch_schema}}.mobile_sessions_metadata_this_run{{.entropy}} (
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

INSERT {{.scratch_schema}}.mobile_sessions_metadata_this_run{{.entropy}} (
  SELECT
    'run',
    run_id,
    '{{.model_version}}',
    'mobile',
    'sessions',
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
  FROM {{.scratch_schema}}.mobile_metadata_run_id{{.entropy}}
);

-- Setup Sessions table
CREATE TABLE IF NOT EXISTS {{.output_schema}}.mobile_sessions{{.entropy}} (

  app_id STRING,
  session_id STRING NOT NULL,
  session_index INT64,
  previous_session_id STRING,
  session_first_event_id STRING,
  session_last_event_id STRING,
  start_tstamp TIMESTAMP,
  end_tstamp TIMESTAMP,
  model_tstamp TIMESTAMP,
  user_id STRING,
  device_user_id STRING,
  network_userid STRING,
  session_duration_s INT64,
  has_install BOOLEAN,
  screen_views INT64,
  screen_names_viewed INT64,
  app_errors INT64,
  fatal_app_errors INT64,
  first_event_name STRING,
  last_event_name STRING,
  first_screen_view_name STRING,
  first_screen_view_transition_type STRING,
  first_screen_view_type STRING,
  last_screen_view_name STRING,
  last_screen_view_transition_type STRING,
  last_screen_view_type STRING,
  platform STRING,
  dvce_screenwidth INT64,
  dvce_screenheight INT64,
  device_manufacturer STRING,
  device_model STRING,
  os_type STRING,
  os_version STRING,
  android_idfa STRING,
  apple_idfa STRING,
  apple_idfv STRING,
  open_idfa STRING,
  device_latitude FLOAT64,
  device_longitude FLOAT64,
  device_latitude_longitude_accuracy FLOAT64,
  device_altitude FLOAT64,
  device_altitude_accuracy FLOAT64,
  device_bearing FLOAT64,
  device_speed FLOAT64,
  geo_country STRING,
  geo_region STRING,
  geo_city STRING,
  geo_zipcode STRING,
  geo_latitude FLOAT64,
  geo_longitude FLOAT64,
  geo_region_name STRING,
  geo_timezone STRING,
  user_ipaddress STRING,
  useragent STRING,
  name_tracker STRING,
  v_tracker STRING,
  carrier STRING,
  network_technology STRING,
  network_type STRING,
  first_build STRING,
  last_build STRING,
  first_version STRING,
  last_version STRING

)
PARTITION BY DATE(start_tstamp)
CLUSTER BY {{range $i, $cluster_field := .cluster_by}} {{if lt $i 4}} {{if $i}}, {{end}} {{$cluster_field}} {{end}} {{else}} app_id,device_user_id,session_id {{end}};
--Cluster using `.cluster_by` var, else use defaults. Max 4 cluster by fields allowed

-- Staged manifest table as input to users step
CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.mobile_sessions_userid_manifest_staged{{.entropy}} (
  device_user_id STRING,
  start_tstamp TIMESTAMP
)
PARTITION BY DATE(start_tstamp);
