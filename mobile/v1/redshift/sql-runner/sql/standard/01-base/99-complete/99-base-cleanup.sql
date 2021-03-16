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

{{if eq .cleanup_mode "trace"}} SELECT 1; {{else}}
  DROP TABLE IF EXISTS {{.scratch_schema}}.{{.model}}_base_new_events_limits{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.{{.model}}_base_sessions_to_process{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.{{.model}}_base_sessions_to_include{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.{{.model}}_base_metadata_this_run{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.{{.model}}_base_run_metadata_temp{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.{{.model}}_base_run_dupe_metadata_temp{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.{{.model}}_base_session_id_run_manifest{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.{{.model}}_base_events_to_process{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.{{.model}}_base_event_ids_this_run{{.entropy}};
  {{if eq .model "web"}}
    DROP TABLE IF EXISTS {{.scratch_schema}}.web_events_addon_pv_context{{.entropy}};
  {{else if eq .model "mobile"}}
    DROP TABLE IF EXISTS {{.scratch_schema}}.mobile_events_addon_mobile_context{{.entropy}};
    DROP TABLE IF EXISTS {{.scratch_schema}}.mobile_events_addon_geolocation_context{{.entropy}};
    DROP TABLE IF EXISTS {{.scratch_schema}}.mobile_events_addon_application_context{{.entropy}};
    DROP TABLE IF EXISTS {{.scratch_schema}}.mobile_events_addon_screen_context{{.entropy}};
  {{end}}
{{end}}

{{if eq .cleanup_mode "debug" "trace"}} SELECT 1; {{else}}
  DROP TABLE IF EXISTS {{.scratch_schema}}.{{.model}}_base_run_manifest{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.{{.model}}_events_this_run{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.{{.model}}_base_duplicates_this_run{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.{{.model}}_base_run_limits{{.entropy}};
{{end}}

{{if eq .ends_run true}}
  DROP TABLE IF EXISTS {{.scratch_schema}}.{{.model}}_metadata_run_id{{.entropy}};
{{end}}
