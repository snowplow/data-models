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

-- Use variable to set scan limits
DECLARE LOWER_LIMIT, UPPER_LIMIT TIMESTAMP;

SET (LOWER_LIMIT, UPPER_LIMIT) = (SELECT AS STRUCT lower_limit, upper_limit FROM {{.scratch_schema}}.base_run_limits{{.entropy}});

CREATE OR REPLACE TABLE {{.scratch_schema}}.events_this_run{{.entropy}}
AS(
  -- Without downstream joins, it's safe to dedupe by picking the first event_id found.
  SELECT
    ARRAY_AGG(e ORDER BY e.collector_tstamp LIMIT 1)[OFFSET(0)].*
  FROM (
    SELECT
        a.contexts_com_snowplowanalytics_snowplow_web_page_1_0_0[SAFE_OFFSET(0)].id AS page_view_id,
        a.* EXCEPT(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0)

    FROM
      {{.input_schema}}.events a
    INNER JOIN
      {{.scratch_schema}}.base_sessions_to_include{{.entropy}} b
    ON a.domain_sessionid = b.session_id
    WHERE
      a.collector_tstamp >= LOWER_LIMIT
      AND a.collector_tstamp <= UPPER_LIMIT
  ) e
  GROUP BY
    e.event_id
);

