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

DROP TABLE IF EXISTS {{.scratch_schema}}.base_events_this_run_tmp{{.entropy}};

CREATE TABLE {{.scratch_schema}}.base_events_this_run_tmp{{.entropy}}
  DISTSTYLE KEY
  DISTKEY (event_id)
  SORTKEY (collector_tstamp)
AS(
  SELECT
      a.*

  FROM
    {{.input_schema}}.events a
  INNER JOIN
    {{.scratch_schema}}.base_sessions_to_include{{.entropy}} b
  ON a.domain_sessionid = b.session_id

  WHERE
    a.collector_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.base_run_limits{{.entropy}})
    AND a.collector_tstamp <= (SELECT upper_limit FROM {{.scratch_schema}}.base_run_limits{{.entropy}})
);

-- Create staged event ID table before deduplication, for an accurate manifest.
DROP TABLE IF EXISTS {{.scratch_schema}}.base_event_ids_this_run{{.entropy}};

CREATE TABLE {{.scratch_schema}}.base_event_ids_this_run{{.entropy}} AS(
  SELECT
    event_id,
    collector_tstamp
  FROM {{.scratch_schema}}.base_events_this_run_tmp{{.entropy}}
);
