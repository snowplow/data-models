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

-- Get only those session ids that we'd like to process in this run.
DROP TABLE IF EXISTS {{.scratch_schema}}.{{.model}}_base_sessions_to_include{{.entropy}};

CREATE TABLE {{.scratch_schema}}.{{.model}}_base_sessions_to_include{{.entropy}}
  DISTSTYLE KEY
  DISTKEY (session_id)
  SORTKEY (session_id)
AS(
  SELECT
    a.session_id,
    LEAST(a.min_tstamp, b.min_tstamp) AS min_tstamp

  FROM
    {{.scratch_schema}}.{{.model}}_base_sessions_to_process{{.entropy}} a
  LEFT JOIN
    {{.scratch_schema}}.{{.model}}_base_session_id_run_manifest{{.entropy}} b
    ON a.session_id = b.session_id

  WHERE
    a.session_id IS NOT NULL
    AND DATEDIFF(DAY, NVL(b.min_tstamp, a.max_tstamp), a.max_tstamp) <= {{or .days_late_allowed 3}}
    -- Compares the max_tstamp of new data to the min_tstamp for its existing session, if one exists.

);
