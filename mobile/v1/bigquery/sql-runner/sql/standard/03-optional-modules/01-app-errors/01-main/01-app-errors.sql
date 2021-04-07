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

{{if eq (or .enabled false) true}}
  -- App errors events schema evolved over time. Finding all versions of the column
  DECLARE APP_ERRORS_EVENTS_COLUMNS, APP_ERRORS_QUERY STRING;
  CALL {{.output_schema}}.mobile_app_errors_fields(APP_ERRORS_EVENTS_COLUMNS);

  SET APP_ERRORS_QUERY = format("""
    CREATE OR REPLACE TABLE {{.scratch_schema}}.mobile_app_errors_this_run{{.entropy}}
    PARTITION BY DATE(derived_tstamp)
    AS (

      SELECT
        e.event_id,

        e.app_id,

        e.user_id,
        e.device_user_id,
        e.network_userid,

        e.session_id,
        e.session_index,
        e.previous_session_id,
        e.session_first_event_id,

        e.dvce_created_tstamp,
        e.collector_tstamp,
        e.derived_tstamp,
        CURRENT_TIMESTAMP() AS model_tstamp,

        e.platform,
        e.dvce_screenwidth,
        e.dvce_screenheight,
        e.device_manufacturer,
        e.device_model,
        e.os_type,
        e.os_version,
        e.android_idfa,
        e.apple_idfa,
        e.apple_idfv,
        e.open_idfa,

        e.screen_id,
        e.screen_name,
        e.screen_activity,
        e.screen_fragment,
        e.screen_top_view_controller,
        e.screen_type,
        e.screen_view_controller,

        e.device_latitude,
        e.device_longitude,
        e.device_latitude_longitude_accuracy,
        e.device_altitude,
        e.device_altitude_accuracy,
        e.device_bearing,
        e.device_speed,
        e.geo_country,
        e.geo_region,
        e.geo_city,
        e.geo_zipcode,
        e.geo_latitude,
        e.geo_longitude,
        e.geo_region_name,
        e.geo_timezone,

        e.user_ipaddress,
        e.useragent,

        e.carrier,
        e.network_technology,
        e.network_type,

        e.build,
        e.version,
        e.event_index_in_session,

        --Error details
        %s
        

      FROM
        {{.scratch_schema}}.mobile_events_staged{{.entropy}} e

      WHERE 
        e.event_name = 'application_error'
     
    );""", APP_ERRORS_EVENTS_COLUMNS);

    EXECUTE IMMEDIATE APP_ERRORS_QUERY;

{{else}}

  SELECT 1;

{{end}}
