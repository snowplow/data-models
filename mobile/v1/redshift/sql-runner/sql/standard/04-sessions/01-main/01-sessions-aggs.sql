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

DROP TABLE IF EXISTS {{.scratch_schema}}.mobile_sessions_aggregates{{.entropy}};

CREATE TABLE {{.scratch_schema}}.mobile_sessions_aggregates{{.entropy}}
  DISTSTYLE KEY
  DISTKEY (session_id)
  SORTKEY (session_id)
AS(

  WITH events AS (
    SELECT
      es.session_id,
      es.event_id,
      es.event_name,
      es.derived_tstamp,
      es.build,
      es.version,
      es.event_index_in_session,
      MAX(es.event_index_in_session) OVER (PARTITION BY es.session_id) AS events_in_session

    FROM
      {{.scratch_schema}}.mobile_events_staged{{.entropy}} es
    )

  , session_aggs AS (
      SELECT
        e.session_id,
        --last dimensions
        MAX(CASE WHEN e.event_index_in_session = e.events_in_session THEN e.build END) AS last_build,
        MAX(CASE WHEN e.event_index_in_session = e.events_in_session THEN e.version END) AS last_version,
        MAX(CASE WHEN e.event_index_in_session = e.events_in_session THEN e.event_name END) AS last_event_name,
        MAX(CASE WHEN e.event_index_in_session = e.events_in_session THEN e.event_id END) AS session_last_event_id,
        -- time
        MIN(e.derived_tstamp) AS start_tstamp,
        MAX(e.derived_tstamp) AS end_tstamp,
        BOOL_OR(e.event_name = 'application_install') has_install

      FROM
        events e

    GROUP BY 1
    )

  , app_errors AS (
      SELECT
        ae.session_id,
        COUNT(DISTINCT ae.event_id) AS app_errors,
        COUNT(DISTINCT CASE WHEN ae.is_fatal THEN ae.event_id END) AS fatal_app_errors

      FROM
        {{.scratch_schema}}.mobile_app_errors_staged{{.entropy}} ae

      GROUP BY 1
    )

  SELECT
    sa.session_id,
    sa.last_build,
    sa.last_version,
    sa.last_event_name,
    sa.session_last_event_id,
    sa.start_tstamp,
    sa.end_tstamp,
    sa.has_install,
    ae.app_errors,
    ae.fatal_app_errors

  FROM
    session_aggs sa
  LEFT JOIN
    app_errors ae
  ON sa.session_id = ae.session_id  
);
