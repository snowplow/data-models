DROP TABLE IF EXISTS {{.scratch_schema}}.sessions_aggregates{{.entropy}};

CREATE TABLE {{.scratch_schema}}.sessions_aggregates{{.entropy}}
  DISTSTYLE KEY
  DISTKEY (domain_sessionid)
  SORTKEY (domain_sessionid)
AS(
  SELECT
    domain_sessionid,
    -- time
    MIN(start_tstamp) AS start_tstamp,
    MAX(end_tstamp) AS end_tstamp,

    -- engagement
    COUNT(DISTINCT page_view_id) AS page_views,
    SUM(engaged_time_in_s) AS engaged_time_in_s

  FROM
    {{.scratch_schema}}.page_views_staged{{.entropy}}

  GROUP BY 1
);
