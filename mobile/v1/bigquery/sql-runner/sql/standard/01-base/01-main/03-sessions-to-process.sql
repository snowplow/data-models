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
DECLARE SESSIONS_TO_PROCESS_QUERY STRING;
{{if eq .model "mobile"}}
  -- Session context schema evolved with time. Finding all versions of column.
  DECLARE SESSION_ID, SESSION_CONTEXT_COLUMNS STRING;
  CALL {{.output_schema}}.mobile_session_context_fields(SESSION_ID, SESSION_CONTEXT_COLUMNS);

{{end}}

SET (LOWER_LIMIT, UPPER_LIMIT) = (SELECT AS STRUCT lower_limit, upper_limit FROM {{.scratch_schema}}.{{.model}}_base_new_events_limits{{.entropy}});

SET SESSIONS_TO_PROCESS_QUERY = format("""
  -- Get sessionids for new events
  CREATE OR REPLACE TABLE {{.scratch_schema}}.{{.model}}_base_sessions_to_process{{.entropy}}
  AS(
    SELECT
      {{if eq .model "web"}} a.domain_sessionid {{else if eq .model "mobile"}} %s {{end}} AS session_id,
      MIN(a.collector_tstamp) AS min_tstamp,
      MAX(a.collector_tstamp) AS max_tstamp

    FROM
      {{.input_schema}}.events a
    LEFT JOIN
      {{.scratch_schema}}.{{.model}}_base_run_manifest{{.entropy}} b
      ON a.event_id = b.event_id

    WHERE
      b.event_id IS NULL
      AND a.collector_tstamp >= @lowerLimit
      AND a.collector_tstamp <= @upperLimit
      AND TIMESTAMP_DIFF(a.dvce_sent_tstamp, a.dvce_created_tstamp, DAY) <= {{or .days_late_allowed 3}}
      -- don't process data that's too late
       AND {{if eq .model "web"}} a.domain_sessionid {{else if eq .model "mobile"}} %s {{end}} IS NOT NULL
      -- Filter by platform. Required.
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

      {{if eq (or .derived_tstamp_partitioned false) true}}

        AND a.derived_tstamp >= @lowerLimit
        AND a.derived_tstamp <= @upperLimit

      {{end}}

    GROUP BY 1
  );""" {{if eq .model "mobile"}} , SESSION_ID, SESSION_ID {{end}}); --Only sub strings if mobile model.

EXECUTE IMMEDIATE SESSIONS_TO_PROCESS_QUERY USING LOWER_LIMIT AS lowerLimit, UPPER_LIMIT AS upperLimit;
