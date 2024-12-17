/*
   Copyright 2021-2022 Snowplow Analytics Ltd. All rights reserved.

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


CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.metadata_run_id{{.entropy}} (
  run_id TIMESTAMP_NTZ
);

INSERT INTO {{.scratch_schema}}.metadata_run_id{{.entropy}}
  SELECT
    CURRENT_TIMESTAMP::TIMESTAMP_NTZ
  WHERE
    (SELECT run_id FROM {{.scratch_schema}}.metadata_run_id{{.entropy}} LIMIT 1) IS NULL;

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
CREATE OR REPLACE TABLE {{.scratch_schema}}.pv_metadata_this_run{{.entropy}} (
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

INSERT INTO {{.scratch_schema}}.pv_metadata_this_run{{.entropy}} (
  SELECT
    'run',
    run_id,
    '{{.model_version}}',
    'web',
    'page-views',
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
  FROM {{.scratch_schema}}.metadata_run_id{{.entropy}}
);

-- Create page views table if it doesn't exist
CREATE TABLE IF NOT EXISTS {{.output_schema}}.page_views{{.entropy}} (

  page_view_id                     VARCHAR      NOT NULL,
  event_id                         VARCHAR      NOT NULL,

  app_id                           VARCHAR,

  user_id                          VARCHAR,
  domain_userid                    VARCHAR,
  network_userid                   VARCHAR,

  domain_sessionid                 VARCHAR,
  domain_sessionidx                INTEGER,
  page_view_in_session_index       INTEGER,
  page_views_in_session            INTEGER,

  dvce_created_tstamp              TIMESTAMP_NTZ,
  collector_tstamp                 TIMESTAMP_NTZ,
  derived_tstamp                   TIMESTAMP_NTZ,
  start_tstamp                     TIMESTAMP_NTZ,
  end_tstamp                       TIMESTAMP_NTZ,

  engaged_time_in_s                INTEGER,
  absolute_time_in_s               INTEGER,

  horizontal_pixels_scrolled       INTEGER,
  vertical_pixels_scrolled         INTEGER,
  horizontal_percentage_scrolled   DOUBLE PRECISION,
  vertical_percentage_scrolled     DOUBLE PRECISION,

  doc_width                        INTEGER,
  doc_height                       INTEGER,

  page_title                       VARCHAR,
  page_url                         VARCHAR,
  page_urlscheme                   VARCHAR,
  page_urlhost                     VARCHAR,
  page_urlpath                     VARCHAR,
  page_urlquery                    VARCHAR,
  page_urlfragment                 VARCHAR,

  mkt_medium                       VARCHAR,
  mkt_source                       VARCHAR,
  mkt_term                         VARCHAR,
  mkt_content                      VARCHAR,
  mkt_campaign                     VARCHAR,
  mkt_clickid                      VARCHAR,
  mkt_network                      VARCHAR,

  page_referrer                    VARCHAR,
  refr_urlscheme                   VARCHAR,
  refr_urlhost                     VARCHAR,
  refr_urlpath                     VARCHAR,
  refr_urlquery                    VARCHAR,
  refr_urlfragment                 VARCHAR,
  refr_medium                      VARCHAR,
  refr_source                      VARCHAR,
  refr_term                        VARCHAR,

  geo_country                      VARCHAR,
  geo_region                       VARCHAR,
  geo_region_name                  VARCHAR,
  geo_city                         VARCHAR,
  geo_zipcode                      VARCHAR,
  geo_latitude                     DOUBLE PRECISION,
  geo_longitude                    DOUBLE PRECISION,
  geo_timezone                     VARCHAR,

  user_ipaddress                   VARCHAR,

  useragent                        VARCHAR,

  br_lang                          VARCHAR,
  br_viewwidth                     INTEGER,
  br_viewheight                    INTEGER,
  br_colordepth                    VARCHAR,
  br_renderengine                  VARCHAR,
  os_timezone                      VARCHAR,

  -- optional iab fields
  category                         VARCHAR,
  primary_impact                   VARCHAR,
  reason                           VARCHAR,
  spider_or_robot                  BOOLEAN,

  -- optional UA parser fields
  useragent_family                 VARCHAR,
  useragent_major                  VARCHAR,
  useragent_minor                  VARCHAR,
  useragent_patch                  VARCHAR,
  useragent_version                VARCHAR,
  os_family                        VARCHAR,
  os_major                         VARCHAR,
  os_minor                         VARCHAR,
  os_patch                         VARCHAR,
  os_patch_minor                   VARCHAR,
  os_version                       VARCHAR,
  device_family                    VARCHAR,

  -- optional YAUAA fields
  device_class                     VARCHAR,
  agent_class                      VARCHAR,
  agent_name                       VARCHAR,
  agent_name_version               VARCHAR,
  agent_name_version_major         VARCHAR,
  agent_version                    VARCHAR,
  agent_version_major              VARCHAR,
  device_brand                     VARCHAR,
  device_name                      VARCHAR,
  device_version                   VARCHAR,
  layout_engine_class              VARCHAR,
  layout_engine_name               VARCHAR,
  layout_engine_name_version       VARCHAR,
  layout_engine_name_version_major VARCHAR,
  layout_engine_version            VARCHAR,
  layout_engine_version_major      VARCHAR,
  operating_system_class           VARCHAR,
  operating_system_name            VARCHAR,
  operating_system_name_version    VARCHAR,
  operating_system_version         VARCHAR
);

-- Create staging table - acts as input to sessions step
CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.page_views_staged{{.entropy}}
  LIKE {{.output_schema}}.page_views{{.entropy}};
