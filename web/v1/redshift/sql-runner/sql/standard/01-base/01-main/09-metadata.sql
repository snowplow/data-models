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

DROP TABLE IF EXISTS {{.scratch_schema}}.base_run_metadata_temp{{.entropy}};

CREATE TABLE {{.scratch_schema}}.base_run_metadata_temp{{.entropy}} AS (
  SELECT
    'run' AS id,
    count(*) AS rows_this_run,
    'event_id' AS distinct_key,
    count(DISTINCT event_id) AS distinct_key_count,
    'collector_tstamp' AS time_key,
    MIN(collector_tstamp) AS min_time_key,
    MAX(collector_tstamp) AS max_time_key

  FROM
    {{.scratch_schema}}.events_this_run{{.entropy}}
);

UPDATE {{.scratch_schema}}.base_metadata_this_run{{.entropy}}
  SET
    run_end_tstamp = GETDATE(),
    rows_this_run = b.rows_this_run,
    distinct_key = b.distinct_key,
    distinct_key_count = b.distinct_key_count,
    time_key = b.time_key,
    min_time_key = b.min_time_key,
    max_time_key = b.max_time_key
  FROM {{.scratch_schema}}.base_run_metadata_temp{{.entropy}} b
  WHERE {{.scratch_schema}}.base_metadata_this_run{{.entropy}}.id = b.id;

DROP TABLE IF EXISTS {{.scratch_schema}}.base_run_dupe_metadata_temp{{.entropy}};


CREATE TABLE {{.scratch_schema}}.base_run_dupe_metadata_temp{{.entropy}} AS (
  SELECT
    'run' AS id,
    SUM(num_rows) AS duplicate_rows_removed,
    count(DISTINCT event_id) AS distinct_keys_removed
  FROM {{.scratch_schema}}.base_duplicates_this_run{{.entropy}}
);

UPDATE {{.scratch_schema}}.base_metadata_this_run{{.entropy}}
  SET
    duplicate_rows_removed = b.duplicate_rows_removed,
    distinct_keys_removed = b.distinct_keys_removed
  FROM {{.scratch_schema}}.base_run_dupe_metadata_temp{{.entropy}} b
  WHERE {{.scratch_schema}}.base_metadata_this_run{{.entropy}}.id = b.id;
