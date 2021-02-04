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

DROP TABLE IF EXISTS {{.scratch_schema}}.mobile_users_run_metadata_temp{{.entropy}};

CREATE TABLE {{.scratch_schema}}.mobile_users_run_metadata_temp{{.entropy}} AS (
  SELECT
    'run' AS id,
    count(*) AS rows_this_run,
    'device_user_id' AS distinct_key,
    count(DISTINCT device_user_id) AS distinct_key_count,
    'start_tstamp' AS time_key,
    MIN(start_tstamp) AS min_time_key,
    MAX(start_tstamp) AS max_time_key

  FROM
    {{.scratch_schema}}.mobile_users_this_run{{.entropy}}
);

UPDATE {{.scratch_schema}}.mobile_users_metadata_this_run{{.entropy}}
  SET
    rows_this_run = b.rows_this_run,
    distinct_key = b.distinct_key,
    distinct_key_count = b.distinct_key_count,
    time_key = b.time_key,
    min_time_key = b.min_time_key,
    max_time_key = b.max_time_key
  FROM {{.scratch_schema}}.mobile_users_run_metadata_temp{{.entropy}} b
  WHERE {{.scratch_schema}}.mobile_users_metadata_this_run{{.entropy}}.id = b.id;
