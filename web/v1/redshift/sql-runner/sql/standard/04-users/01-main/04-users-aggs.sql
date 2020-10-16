DROP TABLE IF EXISTS {{.scratch_schema}}.users_aggregates{{.entropy}};

CREATE TABLE {{.scratch_schema}}.users_aggregates{{.entropy}}
  DISTSTYLE KEY
  DISTKEY (domain_userid)
  SORTKEY (domain_userid)
AS(
  SELECT
    domain_userid,

    -- time
    MIN(start_tstamp) AS start_tstamp,
    MAX(end_tstamp) AS end_tstamp,

    -- engagement
    SUM(page_views) AS page_views,
    COUNT(DISTINCT domain_sessionid) AS sessions,
    SUM(engaged_time_in_s) AS engaged_time_in_s

  FROM {{.scratch_schema}}.users_sessions_this_run{{.entropy}}

  GROUP BY 1
);
