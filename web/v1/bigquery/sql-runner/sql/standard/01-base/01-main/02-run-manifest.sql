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
DECLARE LOWER_LIMIT, SESSION_LIMIT TIMESTAMP;

SET (LOWER_LIMIT, SESSION_LIMIT) = (SELECT AS STRUCT lower_limit, session_limit FROM {{.scratch_schema}}.base_new_events_limits{{.entropy}});

-- Subset the manifest for performance.
CREATE OR REPLACE TABLE {{.scratch_schema}}.base_run_manifest{{.entropy}}
AS(
  SELECT
    *

  FROM
    {{.output_schema}}.base_event_id_manifest{{.entropy}}

  WHERE
    collector_tstamp >= TIMESTAMP_SUB(LOWER_LIMIT, INTERVAL 7 DAY)
);

-- Subset session manifest table - should be as long a timeframe as practical
CREATE OR REPLACE TABLE {{.scratch_schema}}.base_session_id_run_manifest{{.entropy}}
AS(
  SELECT
    *

  FROM
    {{.output_schema}}.base_session_id_manifest{{.entropy}}

  WHERE
    min_tstamp >= SESSION_LIMIT
);
