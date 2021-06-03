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

CREATE OR REPLACE PROCEDURE {{.scratch_schema}}.new_events_limits (lookback_window_hours INT64, update_cadence_days INT64, session_lookback_days INT64)
OPTIONS(strict_mode=false)
BEGIN
  -- Create a limit for this run - single value table.
  CREATE OR REPLACE TABLE {{.scratch_schema}}.base_new_events_limits{{.entropy}}
  AS(
    SELECT
      TIMESTAMP_SUB(MAX(collector_tstamp), INTERVAL lookback_window_hours HOUR) AS lower_limit,
      TIMESTAMP_ADD(MAX(collector_tstamp), INTERVAL update_cadence_days DAY) AS upper_limit,
      TIMESTAMP_SUB(MAX(collector_tstamp), INTERVAL session_lookback_days DAY) AS session_limit

    FROM
      {{.output_schema}}.base_event_id_manifest{{.entropy}}
  );
  
  CALL {{.scratch_schema}}.log_model_table('{{.scratch_schema}}.base_new_events_limits{{.entropy}}', 'trace', 'base');
END;
-- TODO: DESIGN DECISION: Put these all in the setup procedure?
