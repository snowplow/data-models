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

-- When base runs, it's always the first module. So it's safe to just truncate here.
TRUNCATE {{.scratch_schema}}.metadata_run_id{{.entropy}};

INSERT INTO {{.scratch_schema}}.metadata_run_id{{.entropy}} (
  SELECT
    CURRENT_TIMESTAMP::TIMESTAMP_NTZ
);

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
CREATE OR REPLACE TABLE {{.scratch_schema}}.base_metadata_this_run{{.entropy}} (
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
    'seed'::VARCHAR AS event_id,
    '{{.start_date}}'::TIMESTAMP_NTZ AS collector_tstamp
);

CREATE TABLE IF NOT EXISTS {{.output_schema}}.base_session_id_manifest{{.entropy}}
AS (
  SELECT
    'seed'::VARCHAR AS session_id,
    '{{.start_date}}'::TIMESTAMP_NTZ AS min_tstamp
);


-- Create staged table
CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.events_staged{{.entropy}} (

  page_view_id                VARCHAR,

  app_id                      VARCHAR,
  platform                    VARCHAR,

  etl_tstamp                  TIMESTAMP_NTZ ,
  collector_tstamp            TIMESTAMP_NTZ    NOT NULL,
  dvce_created_tstamp         TIMESTAMP_NTZ,

  event                       VARCHAR,
  event_id                    VARCHAR      NOT NULL,
  txn_id                      INTEGER,

  name_tracker                VARCHAR,
  v_tracker                   VARCHAR,
  v_collector                 VARCHAR     NOT NULL,
  v_etl                       VARCHAR     NOT NULL,

  user_id                     VARCHAR,
  user_ipaddress              VARCHAR,
  user_fingerprint            VARCHAR,
  domain_userid               VARCHAR,
  domain_sessionidx           INTEGER,
  network_userid              VARCHAR,

  geo_country                 VARCHAR,
  geo_region                  VARCHAR,
  geo_city                    VARCHAR,
  geo_zipcode                 VARCHAR,
  geo_latitude                DOUBLE PRECISION,
  geo_longitude               DOUBLE PRECISION,
  geo_region_name             VARCHAR,

  ip_isp                      VARCHAR,
  ip_organization             VARCHAR,
  ip_domain                   VARCHAR,
  ip_netspeed                 VARCHAR,

  page_url                    VARCHAR,
  page_title                  VARCHAR,
  page_referrer               VARCHAR,

  page_urlscheme              VARCHAR,
  page_urlhost                VARCHAR,
  page_urlport                INTEGER,
  page_urlpath                VARCHAR,
  page_urlquery               VARCHAR,
  page_urlfragment            VARCHAR,

  refr_urlscheme              VARCHAR,
  refr_urlhost                VARCHAR,
  refr_urlport                INTEGER,
  refr_urlpath                VARCHAR,
  refr_urlquery               VARCHAR,
  refr_urlfragment            VARCHAR,

  refr_medium                 VARCHAR,
  refr_source                 VARCHAR,
  refr_term                   VARCHAR,

  mkt_medium                  VARCHAR,
  mkt_source                  VARCHAR,
  mkt_term                    VARCHAR,
  mkt_content                 VARCHAR,
  mkt_campaign                VARCHAR,

  se_category                 VARCHAR,
  se_action                   VARCHAR,
  se_label                    VARCHAR,
  se_property                 VARCHAR,
  se_value                    DOUBLE PRECISION,

  tr_orderid                  VARCHAR,
  tr_affiliation              VARCHAR,
  tr_total                    NUMBER(18,2),
  tr_tax                      NUMBER(18,2),
  tr_shipping                 NUMBER(18,2),
  tr_city                     VARCHAR,
  tr_state                    VARCHAR,
  tr_country                  VARCHAR,
  ti_orderid                  VARCHAR,
  ti_sku                      VARCHAR,
  ti_name                     VARCHAR,
  ti_category                 VARCHAR,
  ti_price                    NUMBER(18,2),
  ti_quantity                 INTEGER,

  pp_xoffset_min              INTEGER,
  pp_xoffset_max              INTEGER,
  pp_yoffset_min              INTEGER,
  pp_yoffset_max              INTEGER,

  useragent                   VARCHAR,

  br_name                     VARCHAR,
  br_family                   VARCHAR,
  br_version                  VARCHAR,
  br_type                     VARCHAR,
  br_renderengine             VARCHAR,
  br_lang                     VARCHAR,
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
  br_colordepth               VARCHAR,
  br_viewwidth                INTEGER,
  br_viewheight               INTEGER,

  os_name                     VARCHAR,
  os_family                   VARCHAR,
  os_manufacturer             VARCHAR,
  os_timezone                 VARCHAR,

  dvce_type                   VARCHAR,
  dvce_ismobile               BOOLEAN,
  dvce_screenwidth            INTEGER,
  dvce_screenheight           INTEGER,

  doc_charset                 VARCHAR,
  doc_width                   INTEGER,
  doc_height                  INTEGER,

  tr_currency                 VARCHAR,
  tr_total_base               NUMBER(18,2),
  tr_tax_base                 NUMBER(18,2),
  tr_shipping_base            NUMBER(18,2),
  ti_currency                 VARCHAR,
  ti_price_base               NUMBER(18,2),
  base_currency               VARCHAR,

  geo_timezone                VARCHAR,

  mkt_clickid                 VARCHAR,
  mkt_network                 VARCHAR,

  etl_tags                    VARCHAR,

  dvce_sent_tstamp            TIMESTAMP_NTZ,

  refr_domain_userid          VARCHAR,
  refr_dvce_tstamp            TIMESTAMP_NTZ,

  domain_sessionid            VARCHAR,

  derived_tstamp              TIMESTAMP_NTZ,

  event_vendor                VARCHAR,
  event_name                  VARCHAR,
  event_format                VARCHAR,
  event_version               VARCHAR,

  event_fingerprint           VARCHAR,

  true_tstamp                 TIMESTAMP_NTZ
);
