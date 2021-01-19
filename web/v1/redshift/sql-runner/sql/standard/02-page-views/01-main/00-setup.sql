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

-- A table storing an identifier for this run of a model - used to identify runs of the model across multiple modules/steps (eg. base, page views share this id per run)
CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.metadata_run_id{{.entropy}} (
    run_id TIMESTAMP
);

INSERT INTO {{.scratch_schema}}.metadata_run_id{{.entropy}} (
  SELECT
    GETDATE()
  WHERE
    -- Only insert if table is empty
    (SELECT run_id FROM {{.scratch_schema}}.metadata_run_id{{.entropy}} LIMIT 1) IS NULL
);

-- Permanent metadata table
CREATE TABLE IF NOT EXISTS {{.output_schema}}.web_model_run_metadata{{.entropy}} (
  run_id TIMESTAMP,
  model_version VARCHAR(64),
  module_name VARCHAR(64),
  step_name VARCHAR(64),
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

-- Setup temp metadata tables for this run
DROP TABLE IF EXISTS {{.scratch_schema}}.pv_metadata_this_run{{.entropy}};

CREATE TABLE {{.scratch_schema}}.pv_metadata_this_run{{.entropy}} (
  id VARCHAR(64),
  run_id TIMESTAMP,
  model_version VARCHAR(64),
  module_name VARCHAR(64),
  step_name VARCHAR(64),
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

INSERT INTO {{.scratch_schema}}.pv_metadata_this_run{{.entropy}} (
  SELECT
    'run',
    run_id,
    '{{.model_version}}',
    'web',
    'page-views',
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
  FROM {{.scratch_schema}}.metadata_run_id{{.entropy}}
);

-- Create page views table if it doesn't exist
CREATE TABLE IF NOT EXISTS {{.output_schema}}.page_views{{.entropy}} (

  page_view_id CHAR(36) ENCODE ZSTD NOT NULL,
  event_id CHAR(36) ENCODE ZSTD,

  app_id VARCHAR(255) ENCODE ZSTD,

  user_id VARCHAR(255) ENCODE ZSTD,
  domain_userid VARCHAR(128) ENCODE ZSTD,
  network_userid VARCHAR(128) ENCODE ZSTD,

  domain_sessionid VARCHAR(128) ENCODE ZSTD,
  domain_sessionidx INT ENCODE ZSTD,
  page_view_in_session_index INT ENCODE ZSTD,
  page_views_in_session INT ENCODE ZSTD,

  dvce_created_tstamp TIMESTAMP ENCODE ZSTD,
  collector_tstamp TIMESTAMP ENCODE ZSTD,
  derived_tstamp TIMESTAMP ENCODE ZSTD,
  start_tstamp TIMESTAMP ENCODE ZSTD,
  end_tstamp TIMESTAMP ENCODE ZSTD,

  engaged_time_in_s INT ENCODE ZSTD,
  absolute_time_in_s INT ENCODE ZSTD,

  horizontal_pixels_scrolled INT ENCODE ZSTD,
  vertical_pixels_scrolled INT ENCODE ZSTD,
  horizontal_percentage_scrolled DOUBLE PRECISION ENCODE ZSTD,
  vertical_percentage_scrolled DOUBLE PRECISION ENCODE ZSTD,

  doc_width INT ENCODE ZSTD,
  doc_height INT ENCODE ZSTD,

  page_title VARCHAR(2000) ENCODE ZSTD,
  page_url VARCHAR(4096) ENCODE ZSTD,
  page_urlscheme VARCHAR(16) ENCODE ZSTD,
  page_urlhost VARCHAR(255) ENCODE ZSTD,
  page_urlpath VARCHAR(3000) ENCODE ZSTD,
  page_urlquery VARCHAR(6000) ENCODE ZSTD,
  page_urlfragment VARCHAR(3000) ENCODE ZSTD,

  mkt_medium VARCHAR(255) ENCODE ZSTD,
  mkt_source VARCHAR(255) ENCODE ZSTD,
  mkt_term VARCHAR(255) ENCODE ZSTD,
  mkt_content VARCHAR(500) ENCODE ZSTD,
  mkt_campaign VARCHAR(255) ENCODE ZSTD,
  mkt_clickid VARCHAR(128) ENCODE ZSTD,
  mkt_network VARCHAR(64) ENCODE ZSTD,

  page_referrer VARCHAR(4096) ENCODE ZSTD,
  refr_urlscheme  VARCHAR(16) ENCODE ZSTD,
  refr_urlhost VARCHAR(255) ENCODE ZSTD,
  refr_urlpath VARCHAR(6000) ENCODE ZSTD,
  refr_urlquery VARCHAR(6000) ENCODE ZSTD,
  refr_urlfragment VARCHAR(3000) ENCODE ZSTD,
  refr_medium VARCHAR(25) ENCODE ZSTD,
  refr_source VARCHAR(50) ENCODE ZSTD,
  refr_term VARCHAR(255) ENCODE ZSTD,

  geo_country CHAR(2) ENCODE ZSTD,
  geo_region CHAR(3) ENCODE ZSTD,
  geo_region_name VARCHAR(100) ENCODE ZSTD,
  geo_city VARCHAR(75) ENCODE ZSTD,
  geo_zipcode VARCHAR(15) ENCODE ZSTD,
  geo_latitude DOUBLE PRECISION ENCODE ZSTD,
  geo_longitude DOUBLE PRECISION ENCODE ZSTD,
  geo_timezone  VARCHAR(64) ENCODE ZSTD,

  user_ipaddress VARCHAR(128) ENCODE ZSTD,

  useragent VARCHAR(1000) ENCODE ZSTD,

  br_lang VARCHAR(255) ENCODE ZSTD,
  br_viewwidth INT ENCODE ZSTD,
  br_viewheight INT ENCODE ZSTD,
  br_colordepth VARCHAR(12),
  br_renderengine VARCHAR(50),
  os_timezone VARCHAR(255),

  -- optional iab fields
  category VARCHAR,
  primary_impact VARCHAR,
  reason VARCHAR,
  spider_or_robot BOOLEAN,

  -- optional UA parser fields
  useragent_family VARCHAR,
  useragent_major VARCHAR,
  useragent_minor VARCHAR,
  useragent_patch VARCHAR,
  useragent_version VARCHAR,
  os_family VARCHAR,
  os_major VARCHAR,
  os_minor VARCHAR,
  os_patch VARCHAR,
  os_patch_minor VARCHAR,
  os_version VARCHAR,
  device_family VARCHAR,

  -- optional YAUAA fields
  device_class VARCHAR,
  agent_class VARCHAR,
  agent_name VARCHAR,
  agent_name_version VARCHAR,
  agent_name_version_major VARCHAR,
  agent_version VARCHAR,
  agent_version_major VARCHAR,
  device_brand VARCHAR,
  device_name VARCHAR,
  device_version VARCHAR,
  layout_engine_class VARCHAR,
  layout_engine_name VARCHAR,
  layout_engine_name_version VARCHAR,
  layout_engine_name_version_major VARCHAR,
  layout_engine_version VARCHAR,
  layout_engine_version_major VARCHAR,
  operating_system_class VARCHAR,
  operating_system_name VARCHAR,
  operating_system_name_version VARCHAR,
  operating_system_version VARCHAR
)
DISTSTYLE KEY
DISTKEY (page_view_id)
SORTKEY (start_tstamp);

-- Create staging table - acts as input to sessions step
CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.page_views_staged{{.entropy}} (LIKE {{.output_schema}}.page_views{{.entropy}});
