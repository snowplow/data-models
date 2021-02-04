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

DROP TABLE IF EXISTS {{.scratch_schema}}.mobile_sv_screen_view_events{{.entropy}};

CREATE TABLE {{.scratch_schema}}.mobile_sv_screen_view_events{{.entropy}}
  DISTSTYLE KEY
  DISTKEY (screen_view_id)
  SORTKEY (derived_tstamp)
AS(
  SELECT

    sv.screen_view_id,
    ev.event_id,

    ev.app_id,

    ev.user_id,
    ev.device_user_id,
    ev.network_userid,

    ev.session_id,
    ev.session_index,
    ev.previous_session_id,
    ev.session_first_event_id,

    ev.dvce_created_tstamp,
    ev.collector_tstamp,
    ev.derived_tstamp,

    sv.screen_view_name,
    sv.screen_view_transition_type,
    sv.screen_view_type,
    ev.screen_fragment,
    ev.screen_top_view_controller,
    ev.screen_view_controller,
    sv.screen_view_previous_id,
    sv.screen_view_previous_name,
    sv.screen_view_previous_type,

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
    ev.open_idfa,
    ev.network_technology,
    ev.network_type,

    ev.build,
    ev.version,

    ROW_NUMBER() OVER (PARTITION BY sv.screen_view_id ORDER BY ev.derived_tstamp) AS screen_view_id_index

  FROM 
    {{.scratch_schema}}.mobile_events_staged{{.entropy}} AS ev

  INNER JOIN
    {{.scratch_schema}}.mobile_screen_view_ids{{.entropy}} sv
    ON ev.event_id = sv.root_id
    AND ev.collector_tstamp = sv.root_tstamp

  WHERE ev.event_name = 'screen_view'
  AND sv.screen_view_id IS NOT NULL
);
