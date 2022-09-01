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

-- When base runs, it's always the first module. So it's safe to just truncate here.
TRUNCATE {{.scratch_schema}}.metadata_run_id{{.entropy}};

INSERT INTO {{.scratch_schema}}.metadata_run_id{{.entropy}} (
  SELECT
    GETDATE()
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

-- Setup temp metadata tables for this run
DROP TABLE IF EXISTS {{.scratch_schema}}.base_metadata_this_run{{.entropy}};

CREATE TABLE {{.scratch_schema}}.base_metadata_this_run{{.entropy}} (
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

INSERT INTO {{.scratch_schema}}.base_metadata_this_run{{.entropy}} (
  SELECT
    'run',
    run_id,
    '{{.model_version}}',
    'base',
    'main',
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


-- Setup manifests
CREATE TABLE IF NOT EXISTS {{.output_schema}}.base_event_id_manifest{{.entropy}} (
  event_id VARCHAR(36),
  collector_tstamp TIMESTAMP
)
DISTSTYLE KEY
DISTKEY (event_id)
SORTKEY (collector_tstamp);

-- Seed table if empty
INSERT INTO {{.output_schema}}.base_event_id_manifest{{.entropy}} (
  SELECT
    'seed'::VARCHAR(36),
    '{{.start_date}}'::TIMESTAMP

  WHERE
    (SELECT collector_tstamp FROM {{.output_schema}}.base_event_id_manifest{{.entropy}} LIMIT 1) IS NULL
    -- ensures that the seed is not re-inserted if the table is populated.
);

CREATE TABLE IF NOT EXISTS {{.output_schema}}.base_session_id_manifest{{.entropy}} (
  session_id VARCHAR(128),
  min_tstamp TIMESTAMP
)
DISTSTYLE KEY
DISTKEY (session_id)
SORTKEY (min_tstamp);

INSERT INTO {{.output_schema}}.base_session_id_manifest{{.entropy}} (
  SELECT
    'seed'::VARCHAR(128),
    '{{.start_date}}'::TIMESTAMP

  WHERE
    (SELECT min_tstamp FROM {{.output_schema}}.base_session_id_manifest{{.entropy}} LIMIT 1) IS NULL
    -- ensures that the seed is not re-inserted if the table is populated.
);

-- Drop staged table:
DROP TABLE IF EXISTS {{.scratch_schema}}.events_staged{{.entropy}};

-- Create staged table:
CREATE TABLE {{.scratch_schema}}.events_staged{{.entropy}}

AS (
    SELECT
        a.*,
        b.id AS page_view_id
    FROM
      {{.input_schema}}.events a
    INNER JOIN
      {{.input_schema}}.com_snowplowanalytics_snowplow_web_page_1 b
    ON a.event_id = b.root_id
    AND a.collector_tstamp = b.root_tstamp
    WHERE collector_tstamp > DATEADD('day', 1, current_date)
);
