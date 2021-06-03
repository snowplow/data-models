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


CREATE OR REPLACE PROCEDURE {{.scratch_schema}}.sessions_to_process (days_late_allowed INT64)
OPTIONS(strict_mode=false)
BEGIN
  -- Use variable to set scan limits
  DECLARE LOWER_LIMIT, UPPER_LIMIT TIMESTAMP;

  SET (LOWER_LIMIT, UPPER_LIMIT) = (SELECT AS STRUCT lower_limit, upper_limit FROM {{.scratch_schema}}.base_new_events_limits{{.entropy}});

  -- Get sessionids for new events
  CREATE OR REPLACE TABLE {{.scratch_schema}}.base_sessions_to_process{{.entropy}}
  AS(
    SELECT
      domain_sessionid AS session_id,
      MIN(a.collector_tstamp) AS min_tstamp,
      MAX(a.collector_tstamp) AS max_tstamp

    FROM
      {{.input_schema}}.events a
    LEFT JOIN
      {{.scratch_schema}}.base_run_manifest{{.entropy}} b
      ON a.event_id = b.event_id

    WHERE
      b.event_id IS NULL
      AND a.collector_tstamp >= LOWER_LIMIT
      AND a.collector_tstamp <= UPPER_LIMIT
      AND a.domain_sessionid IS NOT NULL
      AND TIMESTAMP_DIFF(a.dvce_sent_tstamp, a.dvce_created_tstamp, DAY) <= days_late_allowed
      -- don't process data that's too late

      {{if eq (or .derived_tstamp_partitioned false) true}}
      -- TODO: Can we do this without go templating?
      -- This one is less impactful in fairness, since it doesn't change unless you're redeploying your entire events table

        AND a.derived_tstamp >= LOWER_LIMIT
        AND a.derived_tstamp <= UPPER_LIMIT

      {{end}}

    GROUP BY 1
  );
  CALL {{.scratch_schema}}.log_model_table('{{.scratch_schema}}.base_sessions_to_process{{.entropy}}', 'trace', 'base');
END;
