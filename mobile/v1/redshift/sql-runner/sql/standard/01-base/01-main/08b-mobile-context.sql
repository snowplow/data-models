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

  DROP TABLE IF EXISTS {{.scratch_schema}}.mobile_events_addon_mobile_context{{.entropy}};

  CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.mobile_events_addon_mobile_context{{.entropy}} (
    root_id CHAR(36),
    root_tstamp TIMESTAMP ENCODE ZSTD,
    device_manufacturer VARCHAR(4096) ENCODE ZSTD,
    device_model VARCHAR(4096) ENCODE ZSTD,
    os_type VARCHAR(4096) ENCODE ZSTD,
    os_version VARCHAR(4096) ENCODE ZSTD,
    android_idfa VARCHAR(4096) ENCODE ZSTD,
    apple_idfa VARCHAR(4096) ENCODE ZSTD,
    apple_idfv VARCHAR(4096) ENCODE ZSTD,
    carrier VARCHAR(4096) ENCODE ZSTD,
    open_idfa VARCHAR(4096) ENCODE ZSTD,
    network_technology VARCHAR(4096) ENCODE ZSTD,
    network_type VARCHAR(7) ENCODE ZSTD

  )
  DISTSTYLE KEY
  DISTKEY (root_id)
  SORTKEY (root_tstamp);

  {{if eq .mobile_context true}}

    INSERT INTO {{.scratch_schema}}.mobile_events_addon_mobile_context{{.entropy}} (
      SELECT
        mc.root_id,
        mc.root_tstamp,
        mc.device_manufacturer,
        mc.device_model,
        mc.os_type,
        mc.os_version,
        mc.android_idfa,
        mc.apple_idfa,
        mc.apple_idfv,
        mc.carrier,
        mc.open_idfa,
        mc.network_technology,
        mc.network_type

      FROM {{.input_schema}}.com_snowplowanalytics_snowplow_mobile_context_1 mc

      WHERE mc.root_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.{{.model}}_base_run_limits{{.entropy}})
        AND mc.root_tstamp <= (SELECT upper_limit FROM {{.scratch_schema}}.{{.model}}_base_run_limits{{.entropy}})
    );

  {{end}}

{{else}}
  
  SELECT 1;

{{end}}
