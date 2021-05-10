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

{{if eq (or .enabled false) true}}

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
    model_version              VARCHAR(64),
    model                      VARCHAR(64),
    module                     VARCHAR(64),
    run_start_tstamp           TIMESTAMP_NTZ,
    run_end_tstamp             TIMESTAMP_NTZ,
    rows_this_run              INTEGER,
    distinct_key               VARCHAR(64),
    distinct_key_count         INTEGER,
    time_key                   VARCHAR(64),
    min_time_key               TIMESTAMP_NTZ,
    max_time_key               TIMESTAMP_NTZ,
    duplicate_rows_removed     INTEGER,
    distinct_keys_removed      INTEGER
  );

  -- Setup temp metadata tables for this run
  CREATE OR REPLACE TABLE {{.scratch_schema}}.mobile_app_errors_metadata_this_run{{.entropy}} (
    id                         VARCHAR(64),
    run_id                     TIMESTAMP_NTZ,
    model_version              VARCHAR(64),
    model                      VARCHAR(64),
    module                     VARCHAR(64),
    run_start_tstamp           TIMESTAMP_NTZ,
    run_end_tstamp             TIMESTAMP_NTZ,
    rows_this_run              INTEGER,
    distinct_key               VARCHAR(64),
    distinct_key_count         INTEGER,
    time_key                   VARCHAR(64),
    min_time_key               TIMESTAMP_NTZ,
    max_time_key               TIMESTAMP_NTZ,
    duplicate_rows_removed     INTEGER,
    distinct_keys_removed      INTEGER
  );

  INSERT INTO {{.scratch_schema}}.mobile_app_errors_metadata_this_run{{.entropy}} (
    SELECT
      'run',
      run_id,
      '{{.model_version}}',
      'mobile',
      'app-errors',
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

{{end}}


-- Reversing usual order as derived output is optional but staging is not.
-- Staging table always created even if module disabled. This allows for joins downstream.
CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.mobile_app_errors_staged{{.entropy}} (

  event_id VARCHAR(36) NOT NULL,
  app_id VARCHAR(255),
  user_id VARCHAR(255),
  device_user_id VARCHAR(4096),
  network_userid VARCHAR(128),
  session_id VARCHAR(36),
  session_index INT,
  previous_session_id VARCHAR(36),
  session_first_event_id VARCHAR(36),
  dvce_created_tstamp TIMESTAMP_NTZ,
  collector_tstamp TIMESTAMP_NTZ,
  derived_tstamp TIMESTAMP_NTZ,
  model_tstamp TIMESTAMP_NTZ,
  platform VARCHAR(255),
  dvce_screenwidth INT,
  dvce_screenheight INT,
  device_manufacturer VARCHAR(4096),
  device_model VARCHAR(4096),
  os_type VARCHAR(4096),
  os_version VARCHAR(4096),
  android_idfa VARCHAR(4096),
  apple_idfa VARCHAR(4096),
  apple_idfv VARCHAR(4096),
  open_idfa VARCHAR(4096),
  screen_id VARCHAR(36),
  screen_name VARCHAR,
  screen_activity VARCHAR,
  screen_fragment VARCHAR,
  screen_top_view_controller VARCHAR,
  screen_type VARCHAR,
  screen_view_controller VARCHAR,
  device_latitude DOUBLE PRECISION,
  device_longitude DOUBLE PRECISION,
  device_latitude_longitude_accuracy DOUBLE PRECISION,
  device_altitude DOUBLE PRECISION,
  device_altitude_accuracy DOUBLE PRECISION,
  device_bearing DOUBLE PRECISION,
  device_speed DOUBLE PRECISION,
  geo_country VARCHAR(2),
  geo_region VARCHAR(3),
  geo_city VARCHAR(75),
  geo_zipcode VARCHAR(15),
  geo_latitude DOUBLE PRECISION,
  geo_longitude DOUBLE PRECISION,
  geo_region_name VARCHAR(100),
  geo_timezone VARCHAR(64),
  user_ipaddress VARCHAR(128),
  useragent VARCHAR(1000),
  carrier VARCHAR,
  network_technology VARCHAR,
  network_type VARCHAR(255),
  build VARCHAR(255),
  version VARCHAR(255),
  event_index_in_session INT,
  message VARCHAR(2048),
  programming_language VARCHAR(255),
  class_name VARCHAR(1024),
  exception_name VARCHAR(1024),
  is_fatal BOOLEAN,
  line_number INT,
  stack_trace VARCHAR(8192),
  thread_id INT,
  thread_name VARCHAR(1024)

);

{{if eq (or .enabled false) true}}
  {{if ne (or .skip_derived false) true}}

  CREATE TABLE IF NOT EXISTS {{.output_schema}}.mobile_app_errors{{.entropy}}
    LIKE {{.scratch_schema}}.mobile_app_errors_staged{{.entropy}};

  {{end}}
{{end}}
