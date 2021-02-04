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
{{if eq .model "web"}}

  DROP TABLE IF EXISTS {{.scratch_schema}}.web_events_addon_page_context{{.entropy}};

  CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.web_events_addon_page_context{{.entropy}} (
    root_id CHAR(36),
    root_tstamp TIMESTAMP ENCODE ZSTD,
    id VARCHAR(4096) ENCODE ZSTD)

  DISTSTYLE KEY
  DISTKEY (root_id)
  SORTKEY (root_tstamp);

    INSERT INTO {{.scratch_schema}}.web_events_addon_page_context{{.entropy}} (
      SELECT
        root_id,
        root_tstamp,
        id
      FROM {{.input_schema}}.com_snowplowanalytics_snowplow_web_page_1
      WHERE root_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.{{.model}}_base_run_limits{{.entropy}})
      AND   root_tstamp <= (SELECT upper_limit FROM {{.scratch_schema}}.{{.model}}_base_run_limits{{.entropy}})
    );

{{else}}

  SELECT 1;

{{end}}
