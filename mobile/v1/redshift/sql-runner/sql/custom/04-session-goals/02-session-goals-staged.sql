-- 2. Aggregate with a drop and recompute logic

DROP TABLE IF EXISTS {{.scratch_schema}}.session_goals_staged{{.entropy}};

CREATE TABLE {{.scratch_schema}}.session_goals_staged{{.entropy}} AS (

  WITH goals AS (

    SELECT
      sv.session_id,
      BOOL_OR(sv.screen_view_name = 'registration') AS has_started_registration,
      BOOL_OR(sv.screen_view_name = 'my_account') AS has_completed_registration,
      BOOL_OR(sv.screen_view_name = 'search_results') AS has_used_search,
      BOOL_OR(sv.screen_view_name = 'products') AS has_viewed_products

    FROM
      {{.scratch_schema}}.mobile_screen_views_staged{{.entropy}} sv

    GROUP BY 1

  )

  SELECT
    g.session_id,
    g.has_started_registration,
    g.has_completed_registration,
    g.has_used_search,
    g.has_viewed_products,
    CASE 
      WHEN g.has_started_registration AND g.has_completed_registration
          AND g.has_used_search AND g.has_viewed_products THEN TRUE
    ELSE FALSE END AS has_completed_goals

  FROM goals g

);

