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

-- Not excluding contexts_com_snowplowanalytics_snowplow_web_page_1, unlike SF web model. 
-- When we share this new base logic between the mobile and web models, this extra column will be a breaking change.

CREATE OR REPLACE TABLE {{.scratch_schema}}.{{.model}}_events_this_run{{.entropy}}
AS (

  WITH events AS (
    SELECT
      {{if eq .model "web"}} a.contexts_com_snowplowanalytics_snowplow_web_page_1[0]:id::VARCHAR AS page_view_id, {{end}}
      {{if eq .model "mobile"}}
        -- screen view events
        a.unstruct_event_com_snowplowanalytics_mobile_screen_view_1:id::VARCHAR AS screen_view_id,
        a.unstruct_event_com_snowplowanalytics_mobile_screen_view_1:name::VARCHAR AS screen_view_name,
        a.unstruct_event_com_snowplowanalytics_mobile_screen_view_1:previousId::VARCHAR AS screen_view_previous_id,
        a.unstruct_event_com_snowplowanalytics_mobile_screen_view_1:previousName::VARCHAR AS screen_view_previous_name,
        a.unstruct_event_com_snowplowanalytics_mobile_screen_view_1:previousType::VARCHAR AS screen_view_previous_type,
        a.unstruct_event_com_snowplowanalytics_mobile_screen_view_1:transitionType::VARCHAR AS screen_view_transition_type,
        a.unstruct_event_com_snowplowanalytics_mobile_screen_view_1:type::VARCHAR AS screen_view_type,
        -- session context
        a.contexts_com_snowplowanalytics_snowplow_client_session_1[0]:sessionId::VARCHAR AS session_id,
        a.contexts_com_snowplowanalytics_snowplow_client_session_1[0]:sessionIndex::INT AS session_index,
        a.contexts_com_snowplowanalytics_snowplow_client_session_1[0]:previousSessionId::VARCHAR AS previous_session_id,
        a.contexts_com_snowplowanalytics_snowplow_client_session_1[0]:userId::VARCHAR AS device_user_id,
        a.contexts_com_snowplowanalytics_snowplow_client_session_1[0]:firstEventId::VARCHAR AS session_first_event_id,
        -- mobile context
        {{if eq .mobile_context true}}
          a.contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]:deviceManufacturer::VARCHAR AS device_manufacturer,
          a.contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]:deviceModel::VARCHAR AS device_model,
          a.contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]:osType::VARCHAR AS os_type,
          a.contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]:osVersion::VARCHAR AS os_version,
          a.contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]:androidIdfa::VARCHAR AS android_idfa,
          a.contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]:appleIdfa::VARCHAR AS apple_idfa,
          a.contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]:appleIdfv::VARCHAR AS apple_idfv,
          a.contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]:carrier::VARCHAR AS carrier,
          a.contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]:openIdfa::VARCHAR AS open_idfa,
          a.contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]:networkTechnology::VARCHAR AS network_technology,
          a.contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]:networkType::VARCHAR AS network_type,
        {{else}}
          CAST(NULL AS VARCHAR) AS device_manufacturer,
          CAST(NULL AS VARCHAR) AS device_model,
          CAST(NULL AS VARCHAR) AS os_type,
          CAST(NULL AS VARCHAR) AS os_version,
          CAST(NULL AS VARCHAR) AS android_idfa,
          CAST(NULL AS VARCHAR) AS apple_idfa,
          CAST(NULL AS VARCHAR) AS apple_idfv,
          CAST(NULL AS VARCHAR) AS carrier,
          CAST(NULL AS VARCHAR) AS open_idfa,
          CAST(NULL AS VARCHAR) AS network_technology,
          CAST(NULL AS VARCHAR) AS network_type,
        {{end}}
        -- geo context
        {{if eq .geolocation_context true}}
          a.contexts_com_snowplowanalytics_snowplow_geolocation_context_1[0]:latitude::FLOAT AS device_latitude,
          a.contexts_com_snowplowanalytics_snowplow_geolocation_context_1[0]:longitude::FLOAT AS device_longitude,
          a.contexts_com_snowplowanalytics_snowplow_geolocation_context_1[0]:latitudeLongitudeAccuracy::FLOAT AS device_latitude_longitude_accuracy,
          a.contexts_com_snowplowanalytics_snowplow_geolocation_context_1[0]:altitude::FLOAT AS device_altitude,
          a.contexts_com_snowplowanalytics_snowplow_geolocation_context_1[0]:altitudeAccuracy::FLOAT AS device_altitude_accuracy,
          a.contexts_com_snowplowanalytics_snowplow_geolocation_context_1[0]:bearing::FLOAT AS device_bearing,
          a.contexts_com_snowplowanalytics_snowplow_geolocation_context_1[0]:speed::FLOAT AS device_speed,
        {{else}}
          CAST(NULL AS FLOAT) AS device_latitude,
          CAST(NULL AS FLOAT) AS device_longitude,
          CAST(NULL AS FLOAT) AS device_latitude_longitude_accuracy,
          CAST(NULL AS FLOAT) AS device_altitude,
          CAST(NULL AS FLOAT) AS device_altitude_accuracy,
          CAST(NULL AS FLOAT) AS device_bearing,
          CAST(NULL AS FLOAT) AS device_speed,
        {{end}}
        -- app context
        {{if eq .application_context true}}
          a.contexts_com_snowplowanalytics_mobile_application_1[0]:build::VARCHAR AS build,
          a.contexts_com_snowplowanalytics_mobile_application_1[0]:version::VARCHAR AS version,
        {{else}}
          CAST(NULL AS VARCHAR) AS build,
          CAST(NULL AS VARCHAR) AS version,
        {{end}}
        -- screen context
        {{if eq .screen_context true}}
          a.contexts_com_snowplowanalytics_mobile_screen_1[0]:id::VARCHAR AS screen_id,
          a.contexts_com_snowplowanalytics_mobile_screen_1[0]:name::VARCHAR AS screen_name,
          a.contexts_com_snowplowanalytics_mobile_screen_1[0]:activity::VARCHAR AS screen_activity,
          a.contexts_com_snowplowanalytics_mobile_screen_1[0]:fragment::VARCHAR AS screen_fragment,
          a.contexts_com_snowplowanalytics_mobile_screen_1[0]:topViewController::VARCHAR AS screen_top_view_controller,
          a.contexts_com_snowplowanalytics_mobile_screen_1[0]:type::VARCHAR AS screen_type,
          a.contexts_com_snowplowanalytics_mobile_screen_1[0]:viewController::VARCHAR AS screen_view_controller,
        {{else}}
          CAST(NULL AS VARCHAR) AS screen_id,
          CAST(NULL AS VARCHAR) AS screen_name,
          CAST(NULL AS VARCHAR) AS screen_activity,
          CAST(NULL AS VARCHAR) AS screen_fragment,
          CAST(NULL AS VARCHAR) AS screen_top_view_controller,
          CAST(NULL AS VARCHAR) AS screen_type,
          CAST(NULL AS VARCHAR) AS screen_view_controller,
        {{end}}
      {{end}}
      a.*

    FROM 
      {{.input_schema}}.events AS a
    INNER JOIN 
      {{.scratch_schema}}.{{.model}}_base_sessions_to_include{{.entropy}} AS b
      ON {{if eq .model "web"}} a.domain_sessionid {{else if eq .model "mobile"}} a.contexts_com_snowplowanalytics_snowplow_client_session_1[0]:sessionId::VARCHAR {{end}} = b.session_id

    WHERE 
      a.collector_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.{{.model}}_base_run_limits{{.entropy}})
      AND a.collector_tstamp <= (SELECT upper_limit FROM {{.scratch_schema}}.{{.model}}_base_run_limits{{.entropy}})
      AND a.platform IN (
          {{range $i, $platform := .platform_filters}} {{if $i}}, {{end}} '{{$platform}}'  -- User defined platforms if specified
          {{else}}
          {{if eq .model "web"}} 'web' {{else if eq .model "mobile"}} 'mob' {{end}} --default values
          {{end}}
          )
    {{if .app_id_filters}}
        -- Filter by app_id. Ignore if not specified.
      AND a.app_id IN ( {{range $i, $app_id := .app_id_filters}} {{if $i}}, {{end}} '{{$app_id}}' {{end}} )
    {{end}}
  )
  
  , deduped_events AS (
  SELECT
    e.*

  FROM events e

  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY e.event_id ORDER BY e.collector_tstamp) = 1
  )

  SELECT
    {{if eq .model "mobile"}} ROW_NUMBER() OVER(PARTITION BY d.session_id ORDER BY d.derived_tstamp) AS event_index_in_session, {{end}}
    d.*
    

  FROM
    deduped_events d
);
