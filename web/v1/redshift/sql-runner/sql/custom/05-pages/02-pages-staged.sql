-- 2. Aggregate with a drop and recompute logic

DROP TABLE IF EXISTS {{.scratch_schema}}.pages_staged{{.entropy}};

CREATE TABLE {{.scratch_schema}}.pages_staged{{.entropy}} AS(

  --  using events_staged for other event type

  SELECT
    pv.page_urlhost,

    COUNT(DISTINCT pv.page_view_id) AS page_views,
    SUM(pvj.link_clicks) AS link_clicks,

    COUNT(DISTINCT pv.domain_sessionid) AS sessions,
    COUNT(DISTINCT pv.domain_userid) AS users,

    SUM(pv.engaged_time_in_s) AS engaged_time_in_s,
    SUM(pv.absolute_time_in_s) AS absolute_time_in_s


  FROM {{.scratch_schema}}.page_views_staged{{.entropy}} pv
  LEFT JOIN {{.scratch_schema}}.page_views_join_staged{{.entropy}} pvj
    ON pv.page_view_id = pvj.page_view_id

  WHERE pv.start_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.users_limits{{.entropy}})
  AND   pv.start_tstamp <= (SELECT upper_limit FROM {{.scratch_schema}}.users_limits{{.entropy}})

  GROUP BY 1
  );
