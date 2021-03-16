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

DROP TABLE IF EXISTS {{.scratch_schema}}.{{.model}}_base_events_to_process{{.entropy}};

CREATE TABLE {{.scratch_schema}}.{{.model}}_base_events_to_process{{.entropy}}
  DISTSTYLE KEY
  DISTKEY (event_id)
  SORTKEY (collector_tstamp)
AS(
  {{if eq .model "web"}}

  SELECT
      a.*,
      DENSE_RANK() OVER (PARTITION BY a.event_id ORDER BY a.collector_tstamp) AS event_id_dedupe_index --dense_rank to catch event_ids with dupe tstamps later

  FROM
    {{.input_schema}}.events a
  INNER JOIN
    {{.scratch_schema}}.{{.model}}_base_sessions_to_include{{.entropy}} b
  ON a.domain_sessionid = b.session_id

  WHERE
    a.collector_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.{{.model}}_base_run_limits{{.entropy}})
    AND a.collector_tstamp <= (SELECT upper_limit FROM {{.scratch_schema}}.{{.model}}_base_run_limits{{.entropy}})
    AND a.platform IN (
      {{range $i, $platform := .platform_filters}} {{if $i}}, {{end}} '{{$platform}}' {{else}} 'web' {{end}}
      )
    {{if .app_id_filters}}
    -- Filter by app_id. Ignore if not specified. 
    AND a.app_id IN (
      {{range $i, $app_id := .app_id_filters}} {{if $i}}, {{end}} '{{$app_id}}' {{end}}
      )
    {{end}}
  
  {{end}}

  {{if eq .model "mobile"}}
  -- Deviate from web model methodology here for performance gains.
  -- Dedupe sessions context table rather than events table then join on events in 09-events-this-run.
  -- This removes the need to dedupe both the sessions and events tables using window functions.
  SELECT
    cs.root_id AS event_id,
    cs.root_tstamp AS collector_tstamp,
    cs.session_id,
    cs.session_index,
    cs.previous_session_id,
    cs.user_id AS device_user_id,
    cs.first_event_id AS session_first_event_id,
    DENSE_RANK() OVER (PARTITION BY cs.root_id ORDER BY cs.root_tstamp) AS event_id_dedupe_index --dense_rank to catch event_ids with dupe tstamps later

  FROM
    {{.input_schema}}.com_snowplowanalytics_snowplow_client_session_1 cs
  INNER JOIN
    {{.scratch_schema}}.{{.model}}_base_sessions_to_include{{.entropy}} bs
  ON cs.session_id = bs.session_id

  WHERE
    cs.root_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.{{.model}}_base_run_limits{{.entropy}})
    AND cs.root_tstamp <= (SELECT upper_limit FROM {{.scratch_schema}}.{{.model}}_base_run_limits{{.entropy}})

  {{end}}

);

-- Create staged event ID table before deduplication, for an accurate manifest.
DROP TABLE IF EXISTS {{.scratch_schema}}.{{.model}}_base_event_ids_this_run{{.entropy}};

CREATE TABLE {{.scratch_schema}}.{{.model}}_base_event_ids_this_run{{.entropy}} AS(
  --DISTINCT to avoid fan when joined on sessions_to_process?
  SELECT
    event_id,
    collector_tstamp
  FROM {{.scratch_schema}}.{{.model}}_base_events_to_process{{.entropy}}
);
