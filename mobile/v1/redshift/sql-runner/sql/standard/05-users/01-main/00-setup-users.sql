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
    run_id TIMESTAMP
);

INSERT INTO {{.scratch_schema}}.mobile_metadata_run_id{{.entropy}} (
  SELECT
    GETDATE()
  WHERE
    -- Only insert if table is empty
    (SELECT run_id FROM {{.scratch_schema}}.mobile_metadata_run_id{{.entropy}} LIMIT 1) IS NULL
);

-- Permanent metadata table
CREATE TABLE IF NOT EXISTS {{.output_schema}}.datamodel_metadata{{.entropy}} (
  run_id TIMESTAMP,
  model_version VARCHAR(64),
  model VARCHAR(64),
  module VARCHAR(64),
  run_start_tstamp TIMESTAMP,
  run_end_tstamp TIMESTAMP,
  rows_this_run INT,
  distinct_key VARCHAR(64),
  distinct_key_count INT,
  time_key VARCHAR(64),
  min_time_key TIMESTAMP,
  max_time_key TIMESTAMP,
  duplicate_rows_removed INT,
  distinct_keys_removed INT
);

-- Setup Metadata
DROP TABLE IF EXISTS {{.scratch_schema}}.mobile_users_metadata_this_run{{.entropy}};

CREATE TABLE {{.scratch_schema}}.mobile_users_metadata_this_run{{.entropy}} (
  id VARCHAR(64),
  run_id TIMESTAMP,
  model_version VARCHAR(64),
  model VARCHAR(64),
  module VARCHAR(64),
  run_start_tstamp TIMESTAMP,
  run_end_tstamp TIMESTAMP,
  rows_this_run INT,
  distinct_key VARCHAR(64),
  distinct_key_count INT,
  time_key VARCHAR(64),
  min_time_key TIMESTAMP,
  max_time_key TIMESTAMP,
  duplicate_rows_removed INT,
  distinct_keys_removed INT
);

INSERT INTO {{.scratch_schema}}.mobile_users_metadata_this_run{{.entropy}} (
  SELECT
    'run',
    run_id,
    '{{.model_version}}',
    'mobile',
    'users',
    GETDATE(),
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

CREATE TABLE IF NOT EXISTS {{.output_schema}}.mobile_users_manifest{{.entropy}} (
  device_user_id VARCHAR(36),
  start_tstamp TIMESTAMP
)
DISTSTYLE KEY
DISTKEY (device_user_id)
SORTKEY (start_tstamp);

INSERT INTO {{.output_schema}}.mobile_users_manifest{{.entropy}} (
  SELECT
    'seed'::VARCHAR(36),
    '1970-01-01'::TIMESTAMP

  WHERE
    (SELECT start_tstamp FROM {{.output_schema}}.mobile_users_manifest{{.entropy}} LIMIT 1) IS NULL
    -- ensures that the seed is not re-inserted if the table is populated.
);

CREATE TABLE IF NOT EXISTS {{.output_schema}}.mobile_users{{.entropy}} (

  user_id VARCHAR(255) ENCODE ZSTD,
  device_user_id VARCHAR(4096) ENCODE ZSTD,
  network_userid VARCHAR(128) ENCODE ZSTD,

  start_tstamp TIMESTAMP ENCODE RAW, --raw for sk
  end_tstamp TIMESTAMP ENCODE ZSTD,
  model_tstamp TIMESTAMP ENCODE ZSTD,

  screen_views INT ENCODE ZSTD,
  screen_names_viewed INT ENCODE ZSTD,
  sessions INT ENCODE ZSTD,
  sessions_duration_s INT ENCODE ZSTD,
  active_days INT ENCODE ZSTD,

  app_errors INT ENCODE ZSTD,
  fatal_app_errors INT ENCODE ZSTD,

  first_screen_view_name VARCHAR(4096) ENCODE ZSTD,
  first_screen_view_transition_type VARCHAR(4096) ENCODE ZSTD,
  first_screen_view_type VARCHAR(4096) ENCODE ZSTD,
  last_screen_view_name VARCHAR(4096) ENCODE ZSTD,
  last_screen_view_transition_type VARCHAR(4096) ENCODE ZSTD,
  last_screen_view_type VARCHAR(4096) ENCODE ZSTD,

  platform VARCHAR(255) ENCODE ZSTD,
  dvce_screenwidth INT ENCODE ZSTD,
  dvce_screenheight INT ENCODE ZSTD,
  device_manufacturer VARCHAR(4096) ENCODE ZSTD,
  device_model VARCHAR(4096) ENCODE ZSTD,
  os_type VARCHAR(4096) ENCODE ZSTD,
  first_os_version VARCHAR(4096) ENCODE ZSTD,
  last_os_version VARCHAR(4096) ENCODE ZSTD,
  android_idfa VARCHAR(4096) ENCODE ZSTD,
  apple_idfa VARCHAR(4096) ENCODE ZSTD,
  apple_idfv VARCHAR(4096) ENCODE ZSTD,
  open_idfa VARCHAR(4096) ENCODE ZSTD,

  geo_country CHAR(2) ENCODE ZSTD,
  geo_region CHAR(3) ENCODE ZSTD,
  geo_city VARCHAR(75) ENCODE ZSTD,
  geo_zipcode VARCHAR(15) ENCODE ZSTD,
  geo_latitude DOUBLE PRECISION ENCODE ZSTD,
  geo_longitude DOUBLE PRECISION ENCODE ZSTD,
  geo_region_name VARCHAR(100) ENCODE ZSTD,
  geo_timezone VARCHAR(64) ENCODE ZSTD,

  first_carrier VARCHAR(4096) ENCODE ZSTD,
  last_carrier VARCHAR(4096) ENCODE ZSTD

)
DISTSTYLE KEY
DISTKEY (device_user_id)
SORTKEY (start_tstamp);
