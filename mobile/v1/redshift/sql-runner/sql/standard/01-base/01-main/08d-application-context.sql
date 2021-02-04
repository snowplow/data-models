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
{{if eq .model "mobile"}}

  DROP TABLE IF EXISTS {{.scratch_schema}}.mobile_events_addon_application_context{{.entropy}};

  CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.mobile_events_addon_application_context{{.entropy}} (
    root_id CHAR(36),
    root_tstamp TIMESTAMP ENCODE ZSTD,
    build VARCHAR(255) ENCODE ZSTD,
    version VARCHAR(255) ENCODE ZSTD

  )
  DISTSTYLE KEY
  DISTKEY (root_id)
  SORTKEY (root_tstamp);

  {{if eq .application_context true}}

    INSERT INTO {{.scratch_schema}}.mobile_events_addon_application_context{{.entropy}} (
      SELECT
        ma.root_id,
        ma.root_tstamp,
        ma.build,
        ma.version

      FROM {{.input_schema}}.com_snowplowanalytics_mobile_application_1 ma

      WHERE ma.root_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.{{.model}}_base_run_limits{{.entropy}})
        AND ma.root_tstamp <= (SELECT upper_limit FROM {{.scratch_schema}}.{{.model}}_base_run_limits{{.entropy}})
    );

  {{end}}

{{else}}

  SELECT 1;

{{end}}
