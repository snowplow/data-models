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

  DROP TABLE IF EXISTS {{.scratch_schema}}.mobile_events_addon_screen_context{{.entropy}};

  CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.mobile_events_addon_screen_context{{.entropy}} (
    root_id CHAR(36),
    root_tstamp TIMESTAMP ENCODE ZSTD,
    screen_id CHAR(36) ENCODE ZSTD,
    screen_name VARCHAR(4096) ENCODE ZSTD,
    screen_activity VARCHAR(4096) ENCODE ZSTD,
    screen_fragment VARCHAR(4096) ENCODE ZSTD,
    screen_top_view_controller VARCHAR(4096) ENCODE ZSTD,
    screen_type VARCHAR(4096) ENCODE ZSTD,
    screen_view_controller VARCHAR(4096) ENCODE ZSTD
    )
  DISTSTYLE KEY
  DISTKEY (root_id)
  SORTKEY (root_tstamp);

  {{if eq .screen_context true}}

    INSERT INTO {{.scratch_schema}}.mobile_events_addon_screen_context{{.entropy}} (
      SELECT
        ms.root_id,
        ms.root_tstamp,
        ms.id AS screen_id, --consider renaming. It is associated screen_view_id.
        ms.name AS screen_name,
        ms.activity AS screen_activity,
        ms.fragment AS screen_fragment,
        ms.top_view_controller AS screen_top_view_controller,
        ms.type AS screen_type,
        ms.view_controller AS screen_view_controller

      FROM {{.input_schema}}.com_snowplowanalytics_mobile_screen_1 ms

      WHERE ms.root_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.{{.model}}_base_run_limits{{.entropy}})
        AND ms.root_tstamp <= (SELECT upper_limit FROM {{.scratch_schema}}.{{.model}}_base_run_limits{{.entropy}})
    );

  {{end}}

{{else}}

  SELECT 1;

{{end}}
