-- Prep manifest data for users step
DROP TABLE IF EXISTS {{.scratch_schema}}.sessions_userid_manifest_this_run{{.entropy}};

CREATE TABLE {{.scratch_schema}}.sessions_userid_manifest_this_run{{.entropy}}
  DISTSTYLE KEY
  DISTKEY (domain_userid)
  SORTKEY (domain_userid)
AS(
  SELECT
    domain_userid,
    MIN(start_tstamp) AS min_tstamp

  FROM
    {{.scratch_schema}}.sessions_this_run{{.entropy}}

  GROUP BY 1
)
