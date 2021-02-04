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

DROP TABLE IF EXISTS {{.scratch_schema}}.mobile_sessions_this_run{{.entropy}};

CREATE TABLE {{.scratch_schema}}.mobile_sessions_this_run{{.entropy}}
	DISTSTYLE KEY
	DISTKEY (session_id)
	SORTKEY (start_tstamp)
AS (

	SELECT
		es.app_id,

		es.session_id,
		es.session_index,
		es.previous_session_id,
		es.session_first_event_id, --Straight from tracker. Consider replacing with post dedupe first_event_id.
		sa.session_last_event_id,

		sa.start_tstamp,
		sa.end_tstamp,

		es.user_id,
		es.device_user_id,
		es.network_userid,

		sa.has_install,
		sv.screen_views,
		sv.unique_screen_views,
		sa.app_errors,
		sa.fatal_app_errors,

		es.event_name AS first_event_name,
		sa.last_event_name,

		sv.first_screen_view_name,
		sv.first_screen_view_transition_type,
		sv.first_screen_view_type,

		sv.last_screen_view_name,
		sv.last_screen_view_transition_type,
		sv.last_screen_view_type,

		es.platform,
		es.dvce_screenwidth,
		es.dvce_screenheight,
		es.device_manufacturer,
		es.device_model,
		es.os_type,
		es.os_version,
		es.android_idfa,
		es.apple_idfa,
		es.apple_idfv,
	  es.open_idfa,

		es.device_latitude,
		es.device_longitude,
		es.device_latitude_longitude_accuracy,
		es.device_altitude,
		es.device_altitude_accuracy,
		es.device_bearing,
		es.device_speed,
		es.geo_country,
		es.geo_region,
		es.geo_city,
		es.geo_zipcode,
		es.geo_latitude,
		es.geo_longitude,
		es.geo_region_name,
		es.geo_timezone,

		es.user_ipaddress,

		es.useragent,
		es.name_tracker,
		es.v_tracker,

		es.carrier,
		es.network_technology,
		es.network_type,
		--first/last build/version to measure app updates.
		es.build AS first_build,
		sa.last_build,
		es.version AS first_version,
		sa.last_version

	FROM
		{{.scratch_schema}}.mobile_events_staged{{.entropy}} es
	INNER JOIN {{.scratch_schema}}.mobile_sessions_aggregates{{.entropy}} sa
		ON es.session_id = sa.session_id
	LEFT JOIN --left join as session might not have screen view i.e. app error on opening
		{{.scratch_schema}}.mobile_sessions_screen_view_details{{.entropy}} sv
		ON es.session_id = sv.session_id

	WHERE 
		es.event_index_in_session = 1
		
);