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

DROP TABLE IF EXISTS {{.scratch_schema}}.mobile_screen_view_ids{{.entropy}};

CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.mobile_screen_view_ids{{.entropy}} (
  root_id CHAR(36),
  root_tstamp TIMESTAMP ENCODE ZSTD,
  screen_view_id CHAR(36) ENCODE ZSTD,
  screen_view_name VARCHAR(4096) ENCODE ZSTD,
  screen_view_previous_id CHAR(36) ENCODE ZSTD,
  screen_view_previous_name VARCHAR(4096) ENCODE ZSTD,
  screen_view_previous_type VARCHAR(4096) ENCODE ZSTD,
  screen_view_transition_type VARCHAR(4096) ENCODE ZSTD,
  screen_view_type VARCHAR(4096) ENCODE ZSTD
  )
DISTSTYLE KEY
DISTKEY (root_id)
SORTKEY (root_tstamp);

INSERT INTO {{.scratch_schema}}.mobile_screen_view_ids{{.entropy}} (
  SELECT
    sv.root_id,
    sv.root_tstamp,
    sv.id AS screen_view_id,
    sv.name AS screen_view_name,
    sv.previous_id AS screen_view_previous_id,
    sv.previous_name AS screen_view_previous_name,
    sv.previous_type AS screen_view_previous_type,
    sv.transition_type AS screen_view_transition_type,
    sv.type AS screen_view_type

  FROM {{.input_schema}}.com_snowplowanalytics_mobile_screen_view_1 sv

  WHERE sv.root_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.mobile_sv_run_limits{{.entropy}})
    AND sv.root_tstamp <= (SELECT upper_limit FROM {{.scratch_schema}}.mobile_sv_run_limits{{.entropy}})
);
