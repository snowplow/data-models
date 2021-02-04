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

-- Create a limit for this run - single row table.
DROP TABLE IF EXISTS {{.scratch_schema}}.mobile_sv_run_limits{{.entropy}};

CREATE TABLE {{.scratch_schema}}.mobile_sv_run_limits{{.entropy}} AS (
  SELECT
    MIN(collector_tstamp) AS lower_limit,
    MAX(collector_tstamp) AS upper_limit

  FROM
    {{.scratch_schema}}.mobile_events_staged{{.entropy}}
  WHERE event_name = 'screen_view'
);
