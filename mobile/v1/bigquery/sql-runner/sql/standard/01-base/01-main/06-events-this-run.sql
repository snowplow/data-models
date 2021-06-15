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

-- Use variable to set scan limits
DECLARE LOWER_LIMIT, UPPER_LIMIT TIMESTAMP;
{{if eq .model "mobile"}}
  -- Session and mobile context schema evolved with time. Finding all versions of column.
  DECLARE SESSION_ID, SESSION_CONTEXT_COLUMNS, MOBILE_CONTEXT_COLUMNS, MOBILE_EVENTS_QUERY STRING;
  CALL {{.output_schema}}.mobile_session_context_fields(SESSION_ID, SESSION_CONTEXT_COLUMNS);
  CALL {{.output_schema}}.mobile_mobile_context_fields({{.mobile_context}}, MOBILE_CONTEXT_COLUMNS);
  
{{end}}

SET (LOWER_LIMIT, UPPER_LIMIT) = (SELECT AS STRUCT lower_limit, upper_limit FROM {{.scratch_schema}}.{{.model}}_base_run_limits{{.entropy}});

  {{if eq .model "web"}}
    CREATE OR REPLACE TABLE {{.scratch_schema}}.{{.model}}_events_this_run{{.entropy}}
    AS(
  -- Without downstream joins, it's safe to dedupe by picking the first event_id found.
    SELECT
      ARRAY_AGG(e ORDER BY e.collector_tstamp LIMIT 1)[OFFSET(0)].*
    FROM (
      SELECT
          a.contexts_com_snowplowanalytics_snowplow_web_page_1_0_0[SAFE_OFFSET(0)].id AS page_view_id,
          a.* EXCEPT(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0)

      FROM
        {{.input_schema}}.events a
      INNER JOIN
        {{.scratch_schema}}.{{.model}}_base_sessions_to_include{{.entropy}} b
      ON a.domain_sessionid = b.session_id
      WHERE
        a.collector_tstamp >= LOWER_LIMIT
        AND a.collector_tstamp <= UPPER_LIMIT
        AND a.platform IN ( {{range $i, $platform := .platform_filters}} {{if $i}}, {{end}} '{{$platform}}' {{else}} 'web' {{end}} )
        {{if .app_id_filters}}
        -- Filter by app_id. Ignore if not specified. 
        AND a.app_id IN ( {{range $i, $app_id := .app_id_filters}} {{if $i}}, {{end}} '{{$app_id}}' {{end}} )
        {{end}}

        {{if eq (or .derived_tstamp_partitioned false) true}}

          AND a.derived_tstamp >= LOWER_LIMIT
          AND a.derived_tstamp <= UPPER_LIMIT

        {{end}}

    ) e
    GROUP BY
      e.event_id
    );
  {{end}}

  {{if eq .model "mobile"}}

  SET MOBILE_EVENTS_QUERY = format("""
    CREATE OR REPLACE TABLE {{.scratch_schema}}.{{.model}}_events_this_run{{.entropy}}
    PARTITION BY DATE(collector_tstamp)
    AS(

    WITH events AS (

      SELECT
        -- Screen view event
        a.unstruct_event_com_snowplowanalytics_mobile_screen_view_1_0_0.id AS screen_view_id,
        a.unstruct_event_com_snowplowanalytics_mobile_screen_view_1_0_0.name AS screen_view_name,
        a.unstruct_event_com_snowplowanalytics_mobile_screen_view_1_0_0.previous_id AS screen_view_previous_id,
        a.unstruct_event_com_snowplowanalytics_mobile_screen_view_1_0_0.previous_name AS screen_view_previous_name,
        a.unstruct_event_com_snowplowanalytics_mobile_screen_view_1_0_0.previous_type AS screen_view_previous_type,
        a.unstruct_event_com_snowplowanalytics_mobile_screen_view_1_0_0.transition_type AS screen_view_transition_type,
        a.unstruct_event_com_snowplowanalytics_mobile_screen_view_1_0_0.type AS screen_view_type,
        -- Session context
        %s,
        -- Mobile context
        %s,
        -- Geo context
        {{if eq .geolocation_context true}}
          a.contexts_com_snowplowanalytics_snowplow_geolocation_context_1_1_0[SAFE_OFFSET(0)].latitude AS device_latitude,
          a.contexts_com_snowplowanalytics_snowplow_geolocation_context_1_1_0[SAFE_OFFSET(0)].longitude AS device_longitude,
          a.contexts_com_snowplowanalytics_snowplow_geolocation_context_1_1_0[SAFE_OFFSET(0)].latitude_longitude_accuracy AS device_latitude_longitude_accuracy,
          a.contexts_com_snowplowanalytics_snowplow_geolocation_context_1_1_0[SAFE_OFFSET(0)].altitude AS device_altitude,
          a.contexts_com_snowplowanalytics_snowplow_geolocation_context_1_1_0[SAFE_OFFSET(0)].altitude_accuracy AS device_altitude_accuracy,
          a.contexts_com_snowplowanalytics_snowplow_geolocation_context_1_1_0[SAFE_OFFSET(0)].bearing AS device_bearing,
          a.contexts_com_snowplowanalytics_snowplow_geolocation_context_1_1_0[SAFE_OFFSET(0)].speed AS device_speed,
        {{else}}
          CAST(NULL AS FLOAT64) AS device_latitude,
          CAST(NULL AS FLOAT64) AS device_longitude,
          CAST(NULL AS FLOAT64) AS device_latitude_longitude_accuracy,
          CAST(NULL AS FLOAT64) AS device_altitude,
          CAST(NULL AS FLOAT64) AS device_altitude_accuracy,
          CAST(NULL AS FLOAT64) AS device_bearing,
          CAST(NULL AS FLOAT64) AS device_speed,
        {{end}}
        -- App context
        {{if eq .application_context true}}
          a.contexts_com_snowplowanalytics_mobile_application_1_0_0[SAFE_OFFSET(0)].build,
          a.contexts_com_snowplowanalytics_mobile_application_1_0_0[SAFE_OFFSET(0)].version,
        {{else}}
          CAST(NULL AS STRING) AS build,
          CAST(NULL AS STRING) AS version,
        {{end}}
        -- Screen context
        {{if eq .screen_context true}}
          a.contexts_com_snowplowanalytics_mobile_screen_1_0_0[SAFE_OFFSET(0)].id AS screen_id,
          a.contexts_com_snowplowanalytics_mobile_screen_1_0_0[SAFE_OFFSET(0)].name AS screen_name,
          a.contexts_com_snowplowanalytics_mobile_screen_1_0_0[SAFE_OFFSET(0)].activity AS screen_activity,
          a.contexts_com_snowplowanalytics_mobile_screen_1_0_0[SAFE_OFFSET(0)].fragment AS screen_fragment,
          a.contexts_com_snowplowanalytics_mobile_screen_1_0_0[SAFE_OFFSET(0)].top_view_controller AS screen_top_view_controller,
          a.contexts_com_snowplowanalytics_mobile_screen_1_0_0[SAFE_OFFSET(0)].type AS screen_type,
          a.contexts_com_snowplowanalytics_mobile_screen_1_0_0[SAFE_OFFSET(0)].view_controller AS screen_view_controller,
        {{else}}
          CAST(NULL AS STRING) AS screen_id,
          CAST(NULL AS STRING) AS screen_name,
          CAST(NULL AS STRING) AS screen_activity,
          CAST(NULL AS STRING) AS screen_fragment,
          CAST(NULL AS STRING) AS screen_top_view_controller,
          CAST(NULL AS STRING) AS screen_type,
          CAST(NULL AS STRING) AS screen_view_controller,
        {{end}}
        -- select a.* after contexts to allow for future additional columns to be added to events_staged during UDF commit_table migratation step.
        -- leaving original context arrays in staged table. Future schema versions may contain fields that are interesting to customers but not the standard model. 
        a.* 
        
      FROM
        {{.input_schema}}.events a
      INNER JOIN
        {{.scratch_schema}}.{{.model}}_base_sessions_to_include{{.entropy}} b
        ON %s = b.session_id

      WHERE
        a.collector_tstamp >= @lowerLimit
        AND a.collector_tstamp <= @upperLimit
        AND a.platform IN ( {{range $i, $platform := .platform_filters}} {{if $i}}, {{end}} '{{$platform}}' {{else}} 'mob' {{end}} )
        {{if .app_id_filters}}
        -- Filter by app_id. Ignore if not specified. 
        AND a.app_id IN ( {{range $i, $app_id := .app_id_filters}} {{if $i}}, {{end}} '{{$app_id}}' {{end}} )
        {{end}}

        {{if eq (or .derived_tstamp_partitioned false) true}}

          AND a.derived_tstamp >= @lowerLimit
          AND a.derived_tstamp <= @upperLimit

        {{end}}

      )
    
    , deduped_events AS (
      -- Without downstream joins, it's safe to dedupe by picking the first event_id found.
      SELECT
        ARRAY_AGG(e ORDER BY e.collector_tstamp LIMIT 1)[OFFSET(0)].*
      
      FROM 
        events AS e

      GROUP BY
        e.event_id
      )

    SELECT
      ROW_NUMBER() OVER(PARTITION BY d.session_id ORDER BY d.derived_tstamp) AS event_index_in_session, 
      d.*
    
    FROM
      deduped_events AS d
    );""", SESSION_CONTEXT_COLUMNS, MOBILE_CONTEXT_COLUMNS, SESSION_ID);

    EXECUTE IMMEDIATE MOBILE_EVENTS_QUERY USING LOWER_LIMIT AS lowerLimit, UPPER_LIMIT AS upperLimit;
      
  {{end}}
