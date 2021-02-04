DROP TABLE IF EXISTS {{.scratch_schema}}.mobile_staging_reconciliation{{.entropy}};

CREATE TABLE {{.scratch_schema}}.mobile_staging_reconciliation{{.entropy}}
  DISTSTYLE KEY
  DISTKEY (_pk)
  SORTKEY (_pk)
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
		COUNT(*) AS removed_screen_view_rows

	FROM {{.scratch_schema}}.mobile_sv_screen_view_events{{.entropy}} AS ev

  WHERE ev.screen_view_id_index != 1
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
    NVL(e.screen_view_rows,0) - NVL(sv.screen_view_rows,0) - NVL(svd.removed_screen_view_rows,0) AS ev_to_sv_sv_rows,
    NVL(e.distinct_sv_event_ids,0) - NVL(sv.distinct_sv_event_ids,0) AS ev_to_sv_distinct_event_ids,
    NVL(e.sessions_rows,0) - NVL(s.sessions_rows,0) AS ev_to_sess_session_rows,
    NVL(e.distinct_session_ids,0) - NVL(s.distinct_session_ids,0) AS ev_to_sess_distinct_session_ids,
    NVL(e.distinct_session_ids_w_screen_view,0) - NVL(sv.distinct_session_ids,0) AS ev_to_sv_distinct_session_ids,
    NVL(e.app_error_distinct_event_ids,0) - NVL(ae.distinct_app_errors_event_id,0) AS ev_to_ae_distinct_event_ids,
    NVL(sv.distinct_screen_view_ids,0) -NVL(s.distinct_screen_view_ids,0) AS sv_to_sess_sv_distinct_screen_view_ids,
    NVL(e.app_error_row_count,0) - NVL(ae.app_error_rows,0) AS ev_to_ae_row_count,
    NVL(ae.distinct_app_errors_event_id,0) - NVL(s.app_errors,0) AS ae_to_sess_app_errors

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


