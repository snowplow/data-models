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
CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.mobile_metadata_run_id{{.entropy}} (
  run_id TIMESTAMP_NTZ
);

INSERT INTO {{.scratch_schema}}.mobile_metadata_run_id{{.entropy}}
  SELECT
    CURRENT_TIMESTAMP::TIMESTAMP_NTZ
  WHERE
    (SELECT run_id FROM {{.scratch_schema}}.mobile_metadata_run_id{{.entropy}} LIMIT 1) IS NULL;


-- Permanent metadata table
CREATE TABLE IF NOT EXISTS {{.output_schema}}.datamodel_metadata{{.entropy}} (
  run_id                     TIMESTAMP_NTZ,
  model_version              VARCHAR,
  model                      VARCHAR,
  module                     VARCHAR,
  run_start_tstamp           TIMESTAMP_NTZ,
  run_end_tstamp             TIMESTAMP_NTZ,
  rows_this_run              INTEGER,
  distinct_key               VARCHAR,
  distinct_key_count         INTEGER,
  time_key                   VARCHAR,
  min_time_key               TIMESTAMP_NTZ,
  max_time_key               TIMESTAMP_NTZ,
  duplicate_rows_removed     INTEGER,
  distinct_keys_removed      INTEGER
);


-- Setup temp metadata tables for this run
CREATE OR REPLACE TABLE {{.scratch_schema}}.mobile_sessions_metadata_this_run{{.entropy}} (
  id                         VARCHAR,
  run_id                     TIMESTAMP_NTZ,
  model_version              VARCHAR,
  model                      VARCHAR,
  module                     VARCHAR,
  run_start_tstamp           TIMESTAMP_NTZ,
  run_end_tstamp             TIMESTAMP_NTZ,
  rows_this_run              INTEGER,
  distinct_key               VARCHAR,
  distinct_key_count         INTEGER,
  time_key                   VARCHAR,
  min_time_key               TIMESTAMP_NTZ,
  max_time_key               TIMESTAMP_NTZ,
  duplicate_rows_removed     INTEGER,
  distinct_keys_removed      INTEGER
);

INSERT INTO {{.scratch_schema}}.mobile_sessions_metadata_this_run{{.entropy}} (
  SELECT
    'run',
    run_id,
    '{{.model_version}}',
    'mobile',
    'sessions',
    CURRENT_TIMESTAMP::TIMESTAMP_NTZ,
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
  app_id VARCHAR,
  session_id VARCHAR NOT NULL,
  session_index INT,
  previous_session_id VARCHAR,
  session_first_event_id VARCHAR,
  session_last_event_id VARCHAR,
  start_tstamp TIMESTAMP_NTZ,
  end_tstamp TIMESTAMP_NTZ,
  model_tstamp TIMESTAMP_NTZ,
  user_id VARCHAR,
  device_user_id VARCHAR,
  network_userid VARCHAR,
  session_duration_s INT,
  has_install BOOLEAN,
  screen_views INT,
  screen_names_viewed INT,
  app_errors INT,
  fatal_app_errors INT,
  first_event_name VARCHAR,
  last_event_name VARCHAR,
  first_screen_view_name VARCHAR,
  first_screen_view_transition_type VARCHAR,
  first_screen_view_type VARCHAR,
  last_screen_view_name VARCHAR,
  last_screen_view_transition_type VARCHAR,
  last_screen_view_type VARCHAR,
  platform VARCHAR,
  dvce_screenwidth INT,
  dvce_screenheight INT,
  device_manufacturer VARCHAR,
  device_model VARCHAR,
  os_type VARCHAR,
  os_version VARCHAR,
  android_idfa VARCHAR,
  apple_idfa VARCHAR,
  apple_idfv VARCHAR,
  open_idfa VARCHAR,
  device_latitude FLOAT,
  device_longitude FLOAT,
  device_latitude_longitude_accuracy FLOAT,
  device_altitude FLOAT,
  device_altitude_accuracy FLOAT,
  device_bearing FLOAT,
  device_speed FLOAT,
  geo_country VARCHAR,
  geo_region VARCHAR,
  geo_city VARCHAR,
  geo_zipcode VARCHAR,
  geo_latitude FLOAT,
  geo_longitude FLOAT,
  geo_region_name VARCHAR,
  geo_timezone VARCHAR,
  user_ipaddress VARCHAR,
  useragent VARCHAR,
  name_tracker VARCHAR,
  v_tracker VARCHAR,
  carrier VARCHAR,
  network_technology VARCHAR,
  network_type VARCHAR,
  first_build VARCHAR,
  last_build VARCHAR,
  first_version VARCHAR,
  last_version VARCHAR
);

-- Staged manifest table as input to users step
CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.mobile_sessions_userid_manifest_staged{{.entropy}} (
  device_user_id VARCHAR,
  start_tstamp TIMESTAMP_NTZ
);
