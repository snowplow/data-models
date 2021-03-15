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

	DROP TABLE IF EXISTS {{.scratch_schema}}.mobile_app_errors_this_run{{.entropy}};

	CREATE TABLE {{.scratch_schema}}.mobile_app_errors_this_run{{.entropy}}
	  DISTSTYLE KEY
	  DISTKEY (event_id)
	  SORTKEY (derived_tstamp)
	AS (

		SELECT
			es.event_id,

			es.app_id,

			es.user_id,
			es.device_user_id,
			es.network_userid,

			es.session_id,
			es.session_index,
			es.previous_session_id,
			es.session_first_event_id,

			es.dvce_created_tstamp,
			es.collector_tstamp,
			es.derived_tstamp,

			--Error contexts
			ae.error_message,
			ae.programming_language,
			ae.class_name,
			ae.error_exception_name,
			ae.is_fatal,
			ae.line_number,
			ae.stack_trace,
			ae.thread_id,
			ae.thread_name,
			ae.error_file_name,
			ae.line_column,
			ae.cause_stack_trace,

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

			es.screen_id,
			es.screen_name,
			es.screen_activity,
			es.screen_fragment,
			es.screen_top_view_controller,
			es.screen_type,
			es.screen_view_controller,

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

			es.carrier,
			es.network_technology,
			es.network_type,

			es.build,
			es.version,
			es.event_index_in_session

		FROM
	    {{.scratch_schema}}.mobile_events_staged{{.entropy}} es
	  INNER JOIN
	  	{{.scratch_schema}}.mobile_app_errors_context{{.entropy}} ae
	  	ON es.event_id = ae.root_id
	  	AND es.collector_tstamp = ae.root_tstamp

	  WHERE
	  	es.event_name = 'application_error'
	  	AND es.collector_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.mobile_app_error_run_limits{{.entropy}})
	    AND es.collector_tstamp <= (SELECT upper_limit FROM {{.scratch_schema}}.mobile_app_error_run_limits{{.entropy}})

	);

{{else}}

	SELECT 1;

{{end}}
