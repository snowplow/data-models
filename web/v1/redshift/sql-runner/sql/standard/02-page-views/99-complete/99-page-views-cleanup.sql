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

{{if eq .cleanup_mode "trace"}} SELECT 1; {{else}}
  DROP TABLE IF EXISTS {{.scratch_schema}}.pv_engaged_time{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.pv_scroll_depth{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.pv_metadata_this_run{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.pv_run_dupe_metadata_temp{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.pv_run_metadata_temp{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.pv_addon_iab{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.pv_addon_ua_parser{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.pv_addon_yauaa{{.entropy}};
{{end}}


{{if eq .cleanup_mode "debug" "trace"}} SELECT 1; {{else}}
  DROP TABLE IF EXISTS {{.scratch_schema}}.pv_run_limits{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.pv_page_view_events{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.page_views_this_run{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.pv_page_view_id_duplicates_this_run{{.entropy}};
{{end}}

{{if eq .ends_run true}}
  DROP TABLE IF EXISTS {{.scratch_schema}}.metadata_run_id{{.entropy}};
{{end}}
