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

  DROP TABLE IF EXISTS {{.scratch_schema}}.mobile_events_addon_geolocation_context{{.entropy}};

  CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.mobile_events_addon_geolocation_context{{.entropy}} (
    root_id CHAR(36),
    root_tstamp TIMESTAMP ENCODE ZSTD,
    device_latitude DOUBLE PRECISION,
    device_longitude DOUBLE PRECISION,
    device_latitude_longitude_accuracy DOUBLE PRECISION,
    device_altitude DOUBLE PRECISION,
    device_altitude_accuracy DOUBLE PRECISION,
    device_bearing DOUBLE PRECISION,
    device_speed DOUBLE PRECISION

  )
  DISTSTYLE KEY
  DISTKEY (root_id)
  SORTKEY (root_tstamp);

  {{if eq .geolocation_context true}}

    INSERT INTO {{.scratch_schema}}.mobile_events_addon_geolocation_context{{.entropy}} (
      SELECT
        gc.root_id,
        gc.root_tstamp,
        gc.latitude AS device_latitude,
        gc.longitude AS device_longitude,
        gc.latitude_longitude_accuracy AS device_latitude_longitude_accuracy,
        gc.altitude AS device_altitude,
        gc.altitude_accuracy AS device_altitude_accuracy,
        gc.bearing AS device_bearing,
        gc.speed AS device_speed

      FROM {{.input_schema}}.com_snowplowanalytics_snowplow_geolocation_context_1 gc

      WHERE gc.root_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.{{.model}}_base_run_limits{{.entropy}})
        AND gc.root_tstamp <= (SELECT upper_limit FROM {{.scratch_schema}}.{{.model}}_base_run_limits{{.entropy}})
    );
    
  {{end}}

{{else}}

  SELECT 1;

{{end}}
