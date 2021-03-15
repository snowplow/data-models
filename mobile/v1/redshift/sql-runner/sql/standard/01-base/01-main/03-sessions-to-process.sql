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

-- Get sessionids for new events
DROP TABLE IF EXISTS {{.scratch_schema}}.{{.model}}_base_sessions_to_process{{.entropy}};

CREATE TABLE {{.scratch_schema}}.{{.model}}_base_sessions_to_process{{.entropy}}
  DISTSTYLE KEY
  DISTKEY (session_id)
  SORTKEY (session_id)
AS(
  SELECT
    {{if eq .model "web"}} a.domain_sessionid {{else if eq .model "mobile"}} mob_session.session_id {{end}} AS session_id,
    MIN(a.collector_tstamp) AS min_tstamp,
    MAX(a.collector_tstamp) AS max_tstamp

  FROM
    {{.input_schema}}.events a
  LEFT JOIN
    {{.scratch_schema}}.{{.model}}_base_run_manifest{{.entropy}} b
    ON a.event_id = b.event_id
    AND a.collector_tstamp = b.collector_tstamp
  {{if eq .model "mobile"}}
  LEFT JOIN
    {{.input_schema}}.com_snowplowanalytics_snowplow_client_session_1 AS mob_session
    ON a.event_id = mob_session.root_id
    AND a.collector_tstamp = mob_session.root_tstamp
    AND mob_session.root_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.{{.model}}_base_new_events_limits{{.entropy}}) --Slower to subset then join
    AND mob_session.root_tstamp <= (SELECT upper_limit FROM {{.scratch_schema}}.{{.model}}_base_new_events_limits{{.entropy}}) -- Tested. Twice the performance
  {{end}}

  WHERE
    b.event_id IS NULL
    AND a.collector_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.{{.model}}_base_new_events_limits{{.entropy}})
    AND a.collector_tstamp <= (SELECT upper_limit FROM {{.scratch_schema}}.{{.model}}_base_new_events_limits{{.entropy}})
    AND DATEDIFF(DAY, a.dvce_created_tstamp, a.dvce_sent_tstamp) <= {{or .days_late_allowed 3}}
    -- don't process data that's too late
    {{if eq .model "web"}}
    AND a.domain_sessionid IS NOT NULL
    {{else if eq .model "mobile"}}
    AND mob_session.session_id IS NOT NULL
    {{end}}
    -- Filter by platform. Required.
    AND a.platform IN (
      {{range $i, $platform := .platform_filters}} 
      {{if $i}}, {{end}} '{{$platform}}'  -- User defined platforms if specified
      {{else}}
      {{if eq .model "web"}} 'web' {{else if eq .model "mobile"}} 'mob' {{end}} --default values
      {{end}}
      )
    {{if .app_id_filters}}
    -- Filter by app_id. Ignore if not specified.
    AND a.app_id IN (
      {{range $i, $app_id := .app_id_filters}} {{if $i}}, {{end}} '{{$app_id}}' {{end}}
      )
    {{end}}
  GROUP BY 1
);
