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

DROP TABLE IF EXISTS {{.scratch_schema}}.events_this_run{{.entropy}};

CREATE TABLE {{.scratch_schema}}.events_this_run{{.entropy}}
  DISTSTYLE KEY
  DISTKEY (event_id)
  SORTKEY (collector_tstamp)
AS(
  SELECT
      a.*,
      b.id AS page_view_id

  FROM
    {{.scratch_schema}}.base_events_this_run_tmp{{.entropy}} a
  LEFT JOIN
    (
    SELECT
      root_id,
      root_tstamp,
      id
    FROM {{.input_schema}}.com_snowplowanalytics_snowplow_web_page_1
    WHERE root_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.base_run_limits{{.entropy}})
    AND   root_tstamp <= (SELECT upper_limit FROM {{.scratch_schema}}.base_run_limits{{.entropy}})
    ) b
    -- We deviate in style here in the name of performance.
  ON a.event_id = b.root_id
  AND a.collector_tstamp = b.root_tstamp
);
