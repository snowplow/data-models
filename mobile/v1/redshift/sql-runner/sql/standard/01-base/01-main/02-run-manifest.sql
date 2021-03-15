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

-- Subset the manifest for performance.
DROP TABLE IF EXISTS {{.scratch_schema}}.{{.model}}_base_run_manifest{{.entropy}};

CREATE TABLE {{.scratch_schema}}.{{.model}}_base_run_manifest{{.entropy}}
  DISTSTYLE KEY
  DISTKEY (event_id)
  SORTKEY (collector_tstamp)
AS(
  SELECT
    *

  FROM
    {{.output_schema}}.{{.model}}_base_event_id_manifest{{.entropy}}

  WHERE
    collector_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.{{.model}}_base_new_events_limits{{.entropy}})
);

DROP TABLE IF EXISTS {{.scratch_schema}}.{{.model}}_base_session_id_run_manifest{{.entropy}};

-- subset session manifest table - should be as long a timeframe as practical
CREATE TABLE {{.scratch_schema}}.{{.model}}_base_session_id_run_manifest{{.entropy}}
  DISTSTYLE KEY
  DISTKEY (session_id)
  SORTKEY (min_tstamp)
AS(
  SELECT
    *

  FROM
    {{.output_schema}}.{{.model}}_base_session_id_manifest{{.entropy}}

  WHERE
    min_tstamp >= (SELECT session_limit FROM {{.scratch_schema}}.{{.model}}_base_new_events_limits{{.entropy}})
);
