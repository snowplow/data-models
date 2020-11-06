/*
   Copyright 2020 Snowplow Analytics Ltd. All rights reserved.

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

-- Setup Metadata
DROP TABLE IF EXISTS {{.scratch_schema}}.users_metadata_this_run{{.entropy}};

CREATE TABLE {{.scratch_schema}}.users_metadata_this_run{{.entropy}} (
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

INSERT INTO {{.scratch_schema}}.users_metadata_this_run{{.entropy}} (
  SELECT
    'run',
    run_id,
    '{{.model_version}}',
    'web',
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
  FROM {{.scratch_schema}}.metadata_run_id{{.entropy}}
);

CREATE TABLE IF NOT EXISTS {{.output_schema}}.users_manifest{{.entropy}} (
  domain_userid VARCHAR(36),
  start_tstamp TIMESTAMP
)
DISTSTYLE KEY
DISTKEY (domain_userid)
SORTKEY (start_tstamp);

INSERT INTO {{.output_schema}}.users_manifest{{.entropy}} (
  SELECT
    'seed'::VARCHAR(36),
    '1970-01-01'::TIMESTAMP

  WHERE
    (SELECT start_tstamp FROM {{.output_schema}}.users_manifest{{.entropy}} LIMIT 1) IS NULL
    -- ensures that the seed is not re-inserted if the table is populated.
);

CREATE TABLE IF NOT EXISTS {{.output_schema}}.users{{.entropy}} (

  -- user fields
  user_id VARCHAR(255) ENCODE ZSTD,
  domain_userid VARCHAR(255) ENCODE ZSTD,
  network_userid VARCHAR(255) ENCODE ZSTD,

  start_tstamp TIMESTAMP ENCODE ZSTD,
  end_tstamp TIMESTAMP ENCODE ZSTD,

  page_views INT ENCODE ZSTD,

  sessions INT ENCODE ZSTD,

  engaged_time_in_s INT ENCODE ZSTD,

  -- first page fields
  first_page_title VARCHAR(2000) ENCODE ZSTD,

  first_page_url VARCHAR(4096) ENCODE ZSTD,

  first_page_urlscheme VARCHAR(16) ENCODE ZSTD,
  first_page_urlhost VARCHAR(255) ENCODE ZSTD,
  first_page_urlpath VARCHAR(3000) ENCODE ZSTD,
  first_page_urlquery VARCHAR(6000) ENCODE ZSTD,
  first_page_urlfragment VARCHAR(3000) ENCODE ZSTD,

  last_page_title VARCHAR(2000) ENCODE ZSTD,

  last_page_url VARCHAR(4096) ENCODE ZSTD,

  last_page_urlscheme VARCHAR(16) ENCODE ZSTD,
  last_page_urlhost VARCHAR(255) ENCODE ZSTD,
  last_page_urlpath VARCHAR(3000) ENCODE ZSTD,
  last_page_urlquery VARCHAR(6000) ENCODE ZSTD,
  last_page_urlfragment VARCHAR(3000) ENCODE ZSTD,

  -- referrer fields
  referrer VARCHAR(4096) ENCODE ZSTD,

  refr_urlscheme VARCHAR(16) ENCODE ZSTD,
  refr_urlhost VARCHAR(255) ENCODE ZSTD,
  refr_urlpath VARCHAR(6000) ENCODE ZSTD,
  refr_urlquery VARCHAR(6000) ENCODE ZSTD,
  refr_urlfragment VARCHAR(3000) ENCODE ZSTD,

  refr_medium VARCHAR(25) ENCODE ZSTD,
  refr_source VARCHAR(50) ENCODE ZSTD,
  refr_term VARCHAR(255) ENCODE ZSTD,

  -- marketing fields
  mkt_medium VARCHAR(255) ENCODE ZSTD,
  mkt_source VARCHAR(255) ENCODE ZSTD,
  mkt_term VARCHAR(255) ENCODE ZSTD,
  mkt_content VARCHAR(500) ENCODE ZSTD,
  mkt_campaign VARCHAR(255) ENCODE ZSTD,
  mkt_clickid VARCHAR(128) ENCODE ZSTD,
  mkt_network VARCHAR(64) ENCODE ZSTD

)
DISTSTYLE KEY
DISTKEY (domain_userid)
SORTKEY (start_tstamp);
