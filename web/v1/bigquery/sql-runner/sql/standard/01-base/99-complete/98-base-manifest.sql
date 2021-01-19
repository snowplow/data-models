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
DECLARE LOWER_LIMIT TIMESTAMP;

SET LOWER_LIMIT = (SELECT lower_limit FROM {{.scratch_schema}}.base_run_limits{{.entropy}});

BEGIN
  DELETE
    FROM {{.output_schema}}.base_event_id_manifest{{.entropy}}
    WHERE event_id IN (SELECT event_id FROM {{.scratch_schema}}.events_this_run{{.entropy}})
    AND collector_tstamp >= LOWER_LIMIT;

  INSERT INTO {{.output_schema}}.base_event_id_manifest{{.entropy}} (SELECT event_id, collector_tstamp FROM {{.scratch_schema}}.events_this_run{{.entropy}});

  -- Commit session_id manifest
  DELETE
    FROM {{.output_schema}}.base_session_id_manifest{{.entropy}}
    WHERE session_id IN (SELECT session_id FROM {{.scratch_schema}}.base_sessions_to_include{{.entropy}});
    -- Should this have a limit?

  INSERT INTO {{.output_schema}}.base_session_id_manifest{{.entropy}} (SELECT * FROM {{.scratch_schema}}.base_sessions_to_include{{.entropy}});
END;
