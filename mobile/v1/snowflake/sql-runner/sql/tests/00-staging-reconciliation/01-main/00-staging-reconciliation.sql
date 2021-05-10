
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

CREATE OR REPLACE TABLE {{.scratch_schema}}.mobile_staging_reconciliation{{.entropy}}
AS (

  WITH events AS (
    SELECT
      '1' AS _pk,
      COUNT(DISTINCT event_id) AS distinct_event_ids,
      SUM(CASE WHEN event_name = 'screen_view' THEN 1 END) AS screen_view_rows,
      COUNT(DISTINCT CASE WHEN event_name = 'screen_view' THEN event_id END) AS distinct_sv_event_ids,
      COUNT(DISTINCT session_id) AS distinct_session_ids,
      SUM(CASE WHEN event_index_in_session = 1 THEN 1 END) AS sessions_rows,
      COUNT(DISTINCT CASE WHEN event_name = 'screen_view' THEN session_id END) AS distinct_session_ids_w_screen_view,
      COUNT(DISTINCT CASE WHEN event_name = 'application_error' THEN event_id END) AS app_error_distinct_event_ids,
      SUM(CASE WHEN event_name = 'application_error' THEN 1 END) AS app_error_row_count

    FROM {{.scratch_schema}}.mobile_events_staged{{.entropy}}
    GROUP BY 1
  )

  , screen_views AS (
    SELECT
      '1' AS _pk,
      COUNT(DISTINCT event_id) AS distinct_sv_event_ids,
      COUNT(DISTINCT screen_view_id) AS distinct_screen_view_ids,
      COUNT(DISTINCT session_id) AS distinct_session_ids,
      COUNT(*) AS screen_view_rows

    FROM {{.scratch_schema}}.mobile_screen_views_staged{{.entropy}}
    GROUP BY 1
  )

  --Not valid if screen views run multiple times to staging.
  , screen_view_removed_dupes AS (
    SELECT
      '1' AS _pk,
      COUNT(*) removed_screen_view_rows

    FROM (
      SELECT
        e.screen_view_id,
        ROW_NUMBER() OVER(PARTITION BY e.screen_view_id ORDER BY e.derived_tstamp) AS row_num

      FROM {{.scratch_schema}}.mobile_events_staged e
      WHERE e.event_name = 'screen_view'
      AND e.screen_view_id IS NOT NULL)
    WHERE row_num != 1
    GROUP BY 1
  )

  , app_errors AS (
    SELECT
      '1' AS _pk,
      COUNT(DISTINCT event_id) AS distinct_app_errors_event_id,
      COUNT(*) AS app_error_rows

    FROM {{.scratch_schema}}.mobile_app_errors_staged{{.entropy}}
    GROUP BY 1
  )

  , sessions AS (
    SELECT
      '1' AS _pk,
      COUNT(DISTINCT session_id) AS distinct_session_ids,
      SUM(screen_views) AS distinct_screen_view_ids,
      COUNT(*) AS sessions_rows,
      SUM(app_errors) AS app_errors

    FROM {{.scratch_schema}}.mobile_sessions_this_run{{.entropy}}
    GROUP BY 1
  )

  SELECT
    e._pk,
    IFNULL(e.screen_view_rows,0) - IFNULL(sv.screen_view_rows,0) - IFNULL(svd.removed_screen_view_rows,0) AS ev_to_sv_sv_rows,
    IFNULL(e.distinct_sv_event_ids,0) - IFNULL(sv.distinct_sv_event_ids,0) AS ev_to_sv_distinct_event_ids,
    IFNULL(e.sessions_rows,0) - IFNULL(s.sessions_rows,0) AS ev_to_sess_session_rows,
    IFNULL(e.distinct_session_ids,0) - IFNULL(s.distinct_session_ids,0) AS ev_to_sess_distinct_session_ids,
    IFNULL(e.distinct_session_ids_w_screen_view,0) - IFNULL(sv.distinct_session_ids,0) AS ev_to_sv_distinct_session_ids,
    {{if eq (or .app_errors false) true}}
    --Only evaluate if module enabled
    IFNULL(e.app_error_distinct_event_ids,0) - IFNULL(ae.distinct_app_errors_event_id,0) AS ev_to_ae_distinct_event_ids,
    IFNULL(e.app_error_row_count,0) - IFNULL(ae.app_error_rows,0) AS ev_to_ae_row_count,
    {{else}}
    0 AS ev_to_ae_distinct_event_ids,
    0 AS ev_to_ae_row_count,
    {{end}}
    IFNULL(sv.distinct_screen_view_ids,0) -IFNULL(s.distinct_screen_view_ids,0) AS sv_to_sess_sv_distinct_screen_view_ids,
    IFNULL(ae.distinct_app_errors_event_id,0) - IFNULL(s.app_errors,0) AS ae_to_sess_app_errors

  FROM events e
  LEFT JOIN screen_views sv
  ON e._pk = sv._pk
  LEFT JOIN screen_view_removed_dupes svd
  ON e._pk = svd._pk
  LEFT JOIN app_errors ae
  ON e._pk = ae._pk
  LEFT JOIN sessions s
  ON e._pk = s._pk

);
