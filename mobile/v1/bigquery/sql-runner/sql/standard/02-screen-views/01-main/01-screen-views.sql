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

CREATE OR REPLACE TABLE {{.scratch_schema}}.mobile_screen_views_this_run{{.entropy}}
PARTITION BY DATE(derived_tstamp)
AS (
  WITH staging AS (
    SELECT
      ev.screen_view_id,
      ev.event_id,

      ev.app_id,

      ev.user_id,
      ev.device_user_id,
      ev.network_userid,

      ev.session_id,
      ev.session_index,
      ev.previous_session_id,
      ev.session_first_event_id,

      ROW_NUMBER() OVER (PARTITION BY ev.session_id ORDER BY ev.derived_tstamp) AS screen_view_in_session_index,

      ev.dvce_created_tstamp,
      ev.collector_tstamp,
      ev.derived_tstamp,

      ev.screen_view_name,
      ev.screen_view_transition_type,
      ev.screen_view_type,
      ev.screen_fragment,
      ev.screen_top_view_controller,
      ev.screen_view_controller,
      ev.screen_view_previous_id,
      ev.screen_view_previous_name,
      ev.screen_view_previous_type,

      ev.platform,
      ev.dvce_screenwidth,
      ev.dvce_screenheight,
      ev.device_manufacturer,
      ev.device_model,
      ev.os_type,
      ev.os_version,
      ev.android_idfa,
      ev.apple_idfa,
      ev.apple_idfv,
      ev.open_idfa,

      ev.device_latitude,
      ev.device_longitude,
      ev.device_latitude_longitude_accuracy,
      ev.device_altitude,
      ev.device_altitude_accuracy,
      ev.device_bearing,
      ev.device_speed,
      ev.geo_country,
      ev.geo_region,
      ev.geo_city,
      ev.geo_zipcode,
      ev.geo_latitude,
      ev.geo_longitude,
      ev.geo_region_name,
      ev.geo_timezone,

      ev.user_ipaddress,

      ev.useragent,

      ev.carrier,
      ev.network_technology,
      ev.network_type,

      ev.build,
      ev.version

    FROM (
      SELECT
        ARRAY_AGG(e ORDER BY e.derived_tstamp LIMIT 1)[OFFSET(0)] AS ev
        -- order by matters here since derived_tstamp determines parts of model logic

      FROM {{.scratch_schema}}.mobile_events_staged{{.entropy}} e
      WHERE e.event_name = 'screen_view'
      AND e.screen_view_id IS NOT NULL
      GROUP BY e.screen_view_id
    )
  )

  SELECT
      s.screen_view_id,
      s.event_id,

      s.app_id,

      s.user_id,
      s.device_user_id,
      s.network_userid,

      s.session_id,
      s.session_index,
      s.previous_session_id,
      s.session_first_event_id,

      s.screen_view_in_session_index,
      MAX(s.screen_view_in_session_index) OVER (PARTITION BY s.session_id) AS screen_views_in_session,

      s.dvce_created_tstamp,
      s.collector_tstamp,
      s.derived_tstamp,
      CURRENT_TIMESTAMP() AS model_tstamp,

      s.screen_view_name,
      s.screen_view_transition_type,
      s.screen_view_type,
      s.screen_fragment,
      s.screen_top_view_controller,
      s.screen_view_controller,
      s.screen_view_previous_id,
      s.screen_view_previous_name,
      s.screen_view_previous_type,

      s.platform,
      s.dvce_screenwidth,
      s.dvce_screenheight,
      s.device_manufacturer,
      s.device_model,
      s.os_type,
      s.os_version,
      s.android_idfa,
      s.apple_idfa,
      s.apple_idfv,
      s.open_idfa,

      s.device_latitude,
      s.device_longitude,
      s.device_latitude_longitude_accuracy,
      s.device_altitude,
      s.device_altitude_accuracy,
      s.device_bearing,
      s.device_speed,
      s.geo_country,
      s.geo_region,
      s.geo_city,
      s.geo_zipcode,
      s.geo_latitude,
      s.geo_longitude,
      s.geo_region_name,
      s.geo_timezone,

      s.user_ipaddress,

      s.useragent,

      s.carrier,
      s.network_technology,
      s.network_type,

      s.build,
      s.version

    FROM
      staging AS s
);
