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
AS (
  WITH screen_views AS (
    SELECT
      e.screen_view_id,
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

      e.screen_view_name,
      e.screen_view_transition_type,
      e.screen_view_type,
      e.screen_fragment,
      e.screen_top_view_controller,
      e.screen_view_controller,
      e.screen_view_previous_id,
      e.screen_view_previous_name,
      e.screen_view_previous_type,

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
      e.version

    FROM 
      {{.scratch_schema}}.mobile_events_staged{{.entropy}} e
   
    WHERE 
      e.event_name = 'screen_view'
      AND e.screen_view_id IS NOT NULL

    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY e.screen_view_id ORDER BY e.derived_tstamp) = 1 --take first screen_view_id
  )

  , deduped_screen_views AS (
    SELECT
      s.*,
      ROW_NUMBER() OVER (PARTITION BY s.session_id ORDER BY s.derived_tstamp) AS screen_view_in_session_index

    FROM
      screen_views s
    )

  SELECT
    d.screen_view_id,
    d.event_id,

    d.app_id,

    d.user_id,
    d.device_user_id,
    d.network_userid,

    d.session_id,
    d.session_index,
    d.previous_session_id,
    d.session_first_event_id,

    d.screen_view_in_session_index,
    MAX(d.screen_view_in_session_index) OVER (PARTITION BY d.session_id) AS screen_views_in_session,

    d.dvce_created_tstamp,
    d.collector_tstamp,
    d.derived_tstamp,
    CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS model_tstamp,

    d.screen_view_name,
    d.screen_view_transition_type,
    d.screen_view_type,
    d.screen_fragment,
    d.screen_top_view_controller,
    d.screen_view_controller,
    d.screen_view_previous_id,
    d.screen_view_previous_name,
    d.screen_view_previous_type,

    d.platform,
    d.dvce_screenwidth,
    d.dvce_screenheight,
    d.device_manufacturer,
    d.device_model,
    d.os_type,
    d.os_version,
    d.android_idfa,
    d.apple_idfa,
    d.apple_idfv,
    d.open_idfa,

    d.device_latitude,
    d.device_longitude,
    d.device_latitude_longitude_accuracy,
    d.device_altitude,
    d.device_altitude_accuracy,
    d.device_bearing,
    d.device_speed,
    d.geo_country,
    d.geo_region,
    d.geo_city,
    d.geo_zipcode,
    d.geo_latitude,
    d.geo_longitude,
    d.geo_region_name,
    d.geo_timezone,

    d.user_ipaddress,

    d.useragent,

    d.carrier,
    d.network_technology,
    d.network_type,

    d.build,
    d.version

  FROM
    deduped_screen_views AS d
);
