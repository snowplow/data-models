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
CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.metadata_run_id{{.entropy}} (
  run_id TIMESTAMP_NTZ
);

-- When base runs, it's always the first module. So it's safe to just truncate here.
TRUNCATE {{.scratch_schema}}.metadata_run_id{{.entropy}};

INSERT INTO {{.scratch_schema}}.metadata_run_id{{.entropy}} (
  SELECT
    CURRENT_TIMESTAMP::TIMESTAMP_NTZ
);

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
CREATE OR REPLACE TABLE {{.scratch_schema}}.base_metadata_this_run{{.entropy}} (
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

INSERT INTO {{.scratch_schema}}.base_metadata_this_run{{.entropy}} (
  SELECT
    'run',
    run_id,
    '{{.model_version}}',
    'web',
    'base',
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


-- Setup manifests
CREATE TABLE IF NOT EXISTS {{.output_schema}}.base_event_id_manifest{{.entropy}}
AS (
  SELECT
    'seed'::VARCHAR(36) AS event_id,
    '{{.start_date}}'::TIMESTAMP_NTZ AS collector_tstamp
);

CREATE TABLE IF NOT EXISTS {{.output_schema}}.base_session_id_manifest{{.entropy}}
AS (
  SELECT
    'seed'::VARCHAR(36) AS session_id,
    '{{.start_date}}'::TIMESTAMP_NTZ AS min_tstamp
);


-- Create staged table
CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.events_staged{{.entropy}} (

  page_view_id                VARCHAR(36),

  app_id                      VARCHAR(255),
  platform                    VARCHAR(255),

  etl_tstamp                  TIMESTAMP_NTZ ,
  collector_tstamp            TIMESTAMP_NTZ    NOT NULL,
  dvce_created_tstamp         TIMESTAMP_NTZ,

  event                       VARCHAR(128),
  event_id                    VARCHAR(36)      NOT NULL,
  txn_id                      INTEGER,

  name_tracker                VARCHAR(128),
  v_tracker                   VARCHAR(100),
  v_collector                 VARCHAR(100)     NOT NULL,
  v_etl                       VARCHAR(100)     NOT NULL,

  user_id                     VARCHAR(255),
  user_ipaddress              VARCHAR(128),
  user_fingerprint            VARCHAR(128),
  domain_userid               VARCHAR(128),
  domain_sessionidx           INTEGER,
  network_userid              VARCHAR(128),

  geo_country                 VARCHAR(2),
  geo_region                  VARCHAR(3),
  geo_city                    VARCHAR(75),
  geo_zipcode                 VARCHAR(15),
  geo_latitude                DOUBLE PRECISION,
  geo_longitude               DOUBLE PRECISION,
  geo_region_name             VARCHAR(100),

  ip_isp                      VARCHAR(100),
  ip_organization             VARCHAR(128),
  ip_domain                   VARCHAR(128),
  ip_netspeed                 VARCHAR(100),

  page_url                    VARCHAR(4096),
  page_title                  VARCHAR(2000),
  page_referrer               VARCHAR(4096),

  page_urlscheme              VARCHAR(16),
  page_urlhost                VARCHAR(255),
  page_urlport                INTEGER,
  page_urlpath                VARCHAR(3000),
  page_urlquery               VARCHAR(6000),
  page_urlfragment            VARCHAR(3000),

  refr_urlscheme              VARCHAR(16),
  refr_urlhost                VARCHAR(255),
  refr_urlport                INTEGER,
  refr_urlpath                VARCHAR(6000),
  refr_urlquery               VARCHAR(6000),
  refr_urlfragment            VARCHAR(3000),

  refr_medium                 VARCHAR(25),
  refr_source                 VARCHAR(50),
  refr_term                   VARCHAR(255),

  mkt_medium                  VARCHAR(255),
  mkt_source                  VARCHAR(255),
  mkt_term                    VARCHAR(255),
  mkt_content                 VARCHAR(500),
  mkt_campaign                VARCHAR(255),

  se_category                 VARCHAR(1000),
  se_action                   VARCHAR(1000),
  se_label                    VARCHAR(4096),
  se_property                 VARCHAR(1000),
  se_value                    DOUBLE PRECISION,

  tr_orderid                  VARCHAR(255),
  tr_affiliation              VARCHAR(255),
  tr_total                    NUMBER(18,2),
  tr_tax                      NUMBER(18,2),
  tr_shipping                 NUMBER(18,2),
  tr_city                     VARCHAR(255),
  tr_state                    VARCHAR(255),
  tr_country                  VARCHAR(255),
  ti_orderid                  VARCHAR(255),
  ti_sku                      VARCHAR(255),
  ti_name                     VARCHAR(255),
  ti_category                 VARCHAR(255),
  ti_price                    NUMBER(18,2),
  ti_quantity                 INTEGER,

  pp_xoffset_min              INTEGER,
  pp_xoffset_max              INTEGER,
  pp_yoffset_min              INTEGER,
  pp_yoffset_max              INTEGER,

  useragent                   VARCHAR(1000),

  br_name                     VARCHAR(50),
  br_family                   VARCHAR(50),
  br_version                  VARCHAR(50),
  br_type                     VARCHAR(50),
  br_renderengine             VARCHAR(50),
  br_lang                     VARCHAR(255),
  br_features_pdf             BOOLEAN,
  br_features_flash           BOOLEAN,
  br_features_java            BOOLEAN,
  br_features_director        BOOLEAN,
  br_features_quicktime       BOOLEAN,
  br_features_realplayer      BOOLEAN,
  br_features_windowsmedia    BOOLEAN,
  br_features_gears           BOOLEAN,
  br_features_silverlight     BOOLEAN,
  br_cookies                  BOOLEAN,
  br_colordepth               VARCHAR(12),
  br_viewwidth                INTEGER,
  br_viewheight               INTEGER,

  os_name                     VARCHAR(50),
  os_family                   VARCHAR(50),
  os_manufacturer             VARCHAR(50),
  os_timezone                 VARCHAR(255),

  dvce_type                   VARCHAR(50),
  dvce_ismobile               BOOLEAN,
  dvce_screenwidth            INTEGER,
  dvce_screenheight           INTEGER,

  doc_charset                 VARCHAR(128),
  doc_width                   INTEGER,
  doc_height                  INTEGER,

  tr_currency                 VARCHAR(3),
  tr_total_base               NUMBER(18,2),
  tr_tax_base                 NUMBER(18,2),
  tr_shipping_base            NUMBER(18,2),
  ti_currency                 VARCHAR(3),
  ti_price_base               NUMBER(18,2),
  base_currency               VARCHAR(3),

  geo_timezone                VARCHAR(64),

  mkt_clickid                 VARCHAR(128),
  mkt_network                 VARCHAR(64),

  etl_tags                    VARCHAR(500),

  dvce_sent_tstamp            TIMESTAMP_NTZ,

  refr_domain_userid          VARCHAR(128),
  refr_dvce_tstamp            TIMESTAMP_NTZ,

  domain_sessionid            VARCHAR(128),

  derived_tstamp              TIMESTAMP_NTZ,

  event_vendor                VARCHAR(1000),
  event_name                  VARCHAR(1000),
  event_format                VARCHAR(128),
  event_version               VARCHAR(128),

  event_fingerprint           VARCHAR(128),

  true_tstamp                 TIMESTAMP_NTZ
);
