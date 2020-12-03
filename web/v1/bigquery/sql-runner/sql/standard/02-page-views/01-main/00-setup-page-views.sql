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
CREATE TABLE IF NOT EXISTS {{.output_schema}}.web_model_run_metadata{{.entropy}} (
  run_id TIMESTAMP,
  model_version STRING,
  module_name STRING,
  step_name STRING,
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
CREATE OR REPLACE TABLE {{.scratch_schema}}.pv_metadata_this_run{{.entropy}} (
  id STRING,
  run_id TIMESTAMP,
  model_version STRING,
  module_name STRING,
  step_name STRING,
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

INSERT INTO {{.scratch_schema}}.pv_metadata_this_run{{.entropy}} (
  SELECT
    'run',
    run_id,
    '{{.model_version}}',
    'web',
    'page-views',
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

-- Create page views table if it doesn't exist
CREATE TABLE IF NOT EXISTS {{.output_schema}}.page_views{{.entropy}} (

  page_view_id STRING  NOT NULL,
  event_id STRING,

  app_id STRING,

  user_id STRING,
  domain_userid STRING,
  network_userid STRING,

  domain_sessionid STRING,
  domain_sessionidx INT64,
  page_view_in_session_index INT64,
  page_views_in_session INT64,

  dvce_created_tstamp TIMESTAMP,
  collector_tstamp TIMESTAMP,
  derived_tstamp TIMESTAMP,
  start_tstamp TIMESTAMP,
  end_tstamp TIMESTAMP,

  engaged_time_in_s INT64,
  absolute_time_in_s INT64,

  horizontal_pixels_scrolled INT64,
  vertical_pixels_scrolled INT64,
  horizontal_percentage_scrolled FLOAT64,
  vertical_percentage_scrolled FLOAT64,

  doc_width INT64,
  doc_height INT64,

  page_title STRING,
  page_url STRING,
  page_urlscheme STRING,
  page_urlhost STRING,
  page_urlpath STRING,
  page_urlquery STRING,
  page_urlfragment STRING,

  mkt_medium STRING,
  mkt_source STRING,
  mkt_term STRING,
  mkt_content STRING,
  mkt_campaign STRING,
  mkt_clickid STRING,
  mkt_network STRING,

  page_referrer STRING,
  refr_urlscheme  STRING,
  refr_urlhost STRING,
  refr_urlpath STRING,
  refr_urlquery STRING,
  refr_urlfragment STRING,
  refr_medium STRING,
  refr_source STRING,
  refr_term STRING,

  geo_country STRING,
  geo_region STRING,
  geo_region_name STRING,
  geo_city STRING,
  geo_zipcode STRING,
  geo_latitude FLOAT64,
  geo_longitude FLOAT64,
  geo_timezone  STRING,

  user_ipaddress STRING,

  useragent STRING,

  br_lang STRING,
  br_viewwidth INT64,
  br_viewheight INT64,
  br_colordepth STRING,
  br_renderengine STRING,
  os_timezone STRING,

  -- optional iab fields
  category STRING,
  primary_impact STRING,
  reason STRING,
  spider_or_robot BOOLEAN,

  -- optional UA parser fields
  useragent_family STRING,
  useragent_major STRING,
  useragent_minor STRING,
  useragent_patch STRING,
  useragent_version STRING,
  os_family STRING,
  os_major STRING,
  os_minor STRING,
  os_patch STRING,
  os_patch_minor STRING,
  os_version STRING,
  device_family STRING,

  -- optional YAUAA fields
  device_class STRING,
  agent_class STRING,
  agent_name STRING,
  agent_name_version STRING,
  agent_name_version_major STRING,
  agent_version STRING,
  agent_version_major STRING,
  device_brand STRING,
  device_name STRING,
  device_version STRING,
  layout_engine_class STRING,
  layout_engine_name STRING,
  layout_engine_name_version STRING,
  layout_engine_name_version_major STRING,
  layout_engine_version STRING,
  layout_engine_version_major STRING,
  operating_system_class STRING,
  operating_system_name STRING,
  operating_system_name_version STRING,
  operating_system_version STRING
)
PARTITION BY DATE(start_tstamp)
CLUSTER BY domain_sessionid,user_id,domain_userid;

-- Create staging table - acts as input to sessions step
CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.page_views_staged{{.entropy}} (

  page_view_id STRING  NOT NULL,
  event_id STRING,

  app_id STRING,

  user_id STRING,
  domain_userid STRING,
  network_userid STRING,

  domain_sessionid STRING,
  domain_sessionidx INT64,
  page_view_in_session_index INT64,
  page_views_in_session INT64,

  dvce_created_tstamp TIMESTAMP,
  collector_tstamp TIMESTAMP,
  derived_tstamp TIMESTAMP,
  start_tstamp TIMESTAMP,
  end_tstamp TIMESTAMP,

  engaged_time_in_s INT64,
  absolute_time_in_s INT64,

  horizontal_pixels_scrolled INT64,
  vertical_pixels_scrolled INT64,
  horizontal_percentage_scrolled FLOAT64,
  vertical_percentage_scrolled FLOAT64,

  doc_width INT64,
  doc_height INT64,

  page_title STRING,
  page_url STRING,
  page_urlscheme STRING,
  page_urlhost STRING,
  page_urlpath STRING,
  page_urlquery STRING,
  page_urlfragment STRING,

  mkt_medium STRING,
  mkt_source STRING,
  mkt_term STRING,
  mkt_content STRING,
  mkt_campaign STRING,
  mkt_clickid STRING,
  mkt_network STRING,

  page_referrer STRING,
  refr_urlscheme  STRING,
  refr_urlhost STRING,
  refr_urlpath STRING,
  refr_urlquery STRING,
  refr_urlfragment STRING,
  refr_medium STRING,
  refr_source STRING,
  refr_term STRING,

  geo_country STRING,
  geo_region STRING,
  geo_region_name STRING,
  geo_city STRING,
  geo_zipcode STRING,
  geo_latitude FLOAT64,
  geo_longitude FLOAT64,
  geo_timezone  STRING,

  user_ipaddress STRING,

  useragent STRING,

  br_lang STRING,
  br_viewwidth INT64,
  br_viewheight INT64,
  br_colordepth STRING,
  br_renderengine STRING,
  os_timezone STRING,

  -- optional iab fields
  category STRING,
  primary_impact STRING,
  reason STRING,
  spider_or_robot BOOLEAN,

  -- optional UA parser fields
  useragent_family STRING,
  useragent_major STRING,
  useragent_minor STRING,
  useragent_patch STRING,
  useragent_version STRING,
  os_family STRING,
  os_major STRING,
  os_minor STRING,
  os_patch STRING,
  os_patch_minor STRING,
  os_version STRING,
  device_family STRING,

  -- optional YAUAA fields
  device_class STRING,
  agent_class STRING,
  agent_name STRING,
  agent_name_version STRING,
  agent_name_version_major STRING,
  agent_version STRING,
  agent_version_major STRING,
  device_brand STRING,
  device_name STRING,
  device_version STRING,
  layout_engine_class STRING,
  layout_engine_name STRING,
  layout_engine_name_version STRING,
  layout_engine_name_version_major STRING,
  layout_engine_version STRING,
  layout_engine_version_major STRING,
  operating_system_class STRING,
  operating_system_name STRING,
  operating_system_name_version STRING,
  operating_system_version STRING
)
PARTITION BY DATE(start_tstamp)
CLUSTER BY domain_sessionid,user_id,domain_userid;
