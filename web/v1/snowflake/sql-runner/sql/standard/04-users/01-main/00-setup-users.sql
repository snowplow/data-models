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

-- Setup Metadata
CREATE OR REPLACE TABLE {{.scratch_schema}}.users_metadata_this_run{{.entropy}} (
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

INSERT INTO {{.scratch_schema}}.users_metadata_this_run{{.entropy}} (
  SELECT
    'run',
    run_id,
    '{{.model_version}}',
    'web',
    'users',
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
  FROM
    {{.scratch_schema}}.metadata_run_id{{.entropy}}
);

CREATE TABLE IF NOT EXISTS {{.output_schema}}.users_manifest{{.entropy}}
AS (
  SELECT
    'seed'::VARCHAR(36) AS domain_userid,
    '1970-01-01'::TIMESTAMP_NTZ AS start_tstamp
);

-- Setup Users table
CREATE TABLE IF NOT EXISTS {{.output_schema}}.users{{.entropy}} (

  -- user fields
  user_id                          VARCHAR(255),
  domain_userid                    VARCHAR(128)        NOT NULL,
  network_userid                   VARCHAR(128),

  start_tstamp                     TIMESTAMP_NTZ,
  end_tstamp                       TIMESTAMP_NTZ,

  page_views                       INTEGER,
  sessions                         INTEGER,
  engaged_time_in_s                INTEGER,

  -- first page fields
  first_page_title                 VARCHAR(2000),
  first_page_url                   VARCHAR(4096),
  first_page_urlscheme             VARCHAR(16),
  first_page_urlhost               VARCHAR(255),
  first_page_urlpath               VARCHAR(3000),
  first_page_urlquery              VARCHAR(6000),
  first_page_urlfragment           VARCHAR(3000),

  -- last page fields
  last_page_title                  VARCHAR(2000),
  last_page_url                    VARCHAR(4096),
  last_page_urlscheme              VARCHAR(16),
  last_page_urlhost                VARCHAR(255),
  last_page_urlpath                VARCHAR(3000),
  last_page_urlquery               VARCHAR(6000),
  last_page_urlfragment            VARCHAR(3000),

  -- referrer fields
  referrer                         VARCHAR(4096),
  refr_urlscheme                   VARCHAR(16),
  refr_urlhost                     VARCHAR(255),
  refr_urlpath                     VARCHAR(6000),
  refr_urlquery                    VARCHAR(6000),
  refr_urlfragment                 VARCHAR(3000),
  refr_medium                      VARCHAR(25),
  refr_source                      VARCHAR(50),
  refr_term                        VARCHAR(255),

  -- marketing fields
  mkt_medium                       VARCHAR(255),
  mkt_source                       VARCHAR(255),
  mkt_term                         VARCHAR(255),
  mkt_content                      VARCHAR(500),
  mkt_campaign                     VARCHAR(255),
  mkt_clickid                      VARCHAR(128),
  mkt_network                      VARCHAR(64)
);
