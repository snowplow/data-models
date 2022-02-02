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

CREATE OR REPLACE PROCEDURE {{.scratch_schema}}.create_events_staged()
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
  AS
  $$

  var sql_stmt = `
      SELECT listagg(isc.column_name, ',') WITHIN GROUP (order by isc.ordinal_position)
      FROM information_schema.columns AS isc
      WHERE table_schema=UPPER('{{.input_schema}}')
        AND table_name=UPPER('events')
        AND column_name != UPPER('contexts_com_snowplowanalytics_snowplow_web_page_1');`;

  var res = snowflake.createStatement({sqlText: sql_stmt}).execute();
  res.next();
  var result = res.getColumnValue(1);

  var new_col = 'contexts_com_snowplowanalytics_snowplow_web_page_1[0]:id::varchar(36) AS page_view_id';
  if (result !== '') {
      new_col = new_col + ',';
  }

  var fin_query=`
    CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.events_staged{{.entropy}}
    AS
      SELECT
        ` + new_col + ` ` + result + `
      FROM {{.input_schema}}.events AS a
      WHERE 1=0 ;`;

  snowflake.createStatement({sqlText: fin_query}).execute();

  return 'ok. procedure has run successfully';

  $$
;

CALL {{.scratch_schema}}.create_events_staged();
