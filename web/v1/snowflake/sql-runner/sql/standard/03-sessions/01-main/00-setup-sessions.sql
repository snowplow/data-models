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


-- A table storing an identifier for this run of a model - used to identify runs of the model across multiple modules/steps (eg. base, page views share this id per run)
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
CREATE OR REPLACE TABLE {{.scratch_schema}}.sessions_metadata_this_run{{.entropy}} (
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

INSERT INTO {{.scratch_schema}}.sessions_metadata_this_run{{.entropy}} (
  SELECT
    'run',
    run_id,
    '{{.model_version}}',
    'web',
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
  FROM {{.scratch_schema}}.metadata_run_id{{.entropy}}
);

-- Setup Sessions table
CREATE TABLE IF NOT EXISTS {{.output_schema}}.sessions{{.entropy}} (
  -- app ID
  app_id                           VARCHAR,

  -- session fields
  domain_sessionid                 VARCHAR     NOT NULL,
  domain_sessionidx                INTEGER,

  start_tstamp                     TIMESTAMP_NTZ,
  end_tstamp                       TIMESTAMP_NTZ,

  -- user fields
  user_id                          VARCHAR,
  domain_userid                    VARCHAR,
  network_userid                   VARCHAR,

  page_views                       INTEGER,
  engaged_time_in_s                INTEGER,
  absolute_time_in_s               INTEGER,

  -- first page fields
  first_page_title                 VARCHAR,
  first_page_url                   VARCHAR,
  first_page_urlscheme             VARCHAR,
  first_page_urlhost               VARCHAR,
  first_page_urlpath               VARCHAR,
  first_page_urlquery              VARCHAR,
  first_page_urlfragment           VARCHAR,

  -- last page fields
  last_page_title                  VARCHAR,
  last_page_url                    VARCHAR,
  last_page_urlscheme              VARCHAR,
  last_page_urlhost                VARCHAR,
  last_page_urlpath                VARCHAR,
  last_page_urlquery               VARCHAR,
  last_page_urlfragment            VARCHAR,

  -- referrer fields
  referrer                         VARCHAR,
  refr_urlscheme                   VARCHAR,
  refr_urlhost                     VARCHAR,
  refr_urlpath                     VARCHAR,
  refr_urlquery                    VARCHAR,
  refr_urlfragment                 VARCHAR,
  refr_medium                      VARCHAR,
  refr_source                      VARCHAR,
  refr_term                        VARCHAR,

  -- marketing fields
  mkt_medium                       VARCHAR,
  mkt_source                       VARCHAR,
  mkt_term                         VARCHAR,
  mkt_content                      VARCHAR,
  mkt_campaign                     VARCHAR,
  mkt_clickid                      VARCHAR,
  mkt_network                      VARCHAR,

  -- geo fields
  geo_country                      VARCHAR,
  geo_region                       VARCHAR,
  geo_region_name                  VARCHAR,
  geo_city                         VARCHAR,
  geo_zipcode                      VARCHAR,
  geo_latitude                     DOUBLE PRECISION,
  geo_longitude                    DOUBLE PRECISION,
  geo_timezone                     VARCHAR,

  -- IP address
  user_ipaddress                   VARCHAR,

  -- user agent
  useragent                        VARCHAR,

  br_renderengine                  VARCHAR,
  br_lang                          VARCHAR,

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

-- Staged manifest table as input to users step
CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.sessions_userid_manifest_staged{{.entropy}} (
  domain_userid VARCHAR,
  start_tstamp TIMESTAMP_NTZ
);
