-- 1. Aggregate with a drop and recompute logic

CREATE OR REPLACE TABLE {{.scratch_schema}}.session_goals_staged{{.entropy}} AS (

  WITH goals AS (

    SELECT
      sv.session_id,
      LOGICAL_OR(sv.screen_view_name = 'registration') AS has_started_registration,
      LOGICAL_OR(sv.screen_view_name = 'my_account') AS has_completed_registration,
      LOGICAL_OR(sv.screen_view_name = 'search_results') AS has_used_search,
      LOGICAL_OR(sv.screen_view_name = 'products') AS has_viewed_products

    FROM
      {{.scratch_schema}}.mobile_screen_views_staged{{.entropy}} sv

    GROUP BY 1

  )

  SELECT
    s.session_id,
    s.start_tstamp,
    g.has_started_registration,
    g.has_completed_registration,
    g.has_used_search,
    g.has_viewed_products,
    IF(g.has_started_registration AND g.has_completed_registration AND g.has_used_search AND g.has_viewed_products, TRUE, FALSE) AS has_completed_goals

  FROM
    {{.scratch_schema}}.mobile_sessions_this_run{{.entropy}} AS s --select from mobile_sessions_this_run to get start_tstamp. Screen view might not be start of session
  INNER JOIN goals AS g
    ON s.session_id = g.session_id

);

