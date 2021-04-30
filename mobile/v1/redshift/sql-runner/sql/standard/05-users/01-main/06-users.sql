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

DROP TABLE IF EXISTS {{.scratch_schema}}.mobile_users_this_run{{.entropy}};

CREATE TABLE {{.scratch_schema}}.mobile_users_this_run{{.entropy}}
  DISTSTYLE KEY
  DISTKEY (device_user_id)
  SORTKEY (start_tstamp)
AS (
  SELECT
    -- user fields
    a.user_id,
    a.device_user_id,
    a.network_userid,

    b.start_tstamp,
    b.end_tstamp,
    CURRENT_TIMESTAMP AS model_tstamp,

    -- engagement fields
    b.screen_views,
    b.screen_names_viewed,
    b.sessions,
    b.sessions_duration_s,
    b.active_days,
    --errors
    b.app_errors,
    b.fatal_app_errors,

    -- screen fields
    a.first_screen_view_name,
    a.first_screen_view_transition_type,
    a.first_screen_view_type,

    c.last_screen_view_name,
    c.last_screen_view_transition_type,
    c.last_screen_view_type,

    -- device fields
    a.platform,
    a.dvce_screenwidth,
    a.dvce_screenheight,
    a.device_manufacturer,
    a.device_model,
    a.os_type,
    a.os_version first_os_version,
    c.last_os_version,
    a.android_idfa,
    a.apple_idfa,
    a.apple_idfv,
    a.open_idfa,

    -- geo fields
    a.geo_country,
    a.geo_region,
    a.geo_city,
    a.geo_zipcode,
    a.geo_latitude,
    a.geo_longitude,
    a.geo_region_name,
    a.geo_timezone,

    a.carrier first_carrier,
    c.last_carrier

  FROM {{.scratch_schema}}.mobile_users_aggregates{{.entropy}} AS b

  INNER JOIN {{.scratch_schema}}.mobile_users_sessions_this_run{{.entropy}} AS a
    ON a.device_user_id = b.device_user_id
    AND a.start_tstamp = b.start_tstamp

  INNER JOIN {{.scratch_schema}}.mobile_users_lasts{{.entropy}} c
    ON b.device_user_id = c.device_user_id
);
