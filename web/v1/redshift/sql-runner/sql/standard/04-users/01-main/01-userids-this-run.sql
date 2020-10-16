-- Create a limit for this run - single value table.
DROP TABLE IF EXISTS {{.scratch_schema}}.users_userids_this_run{{.entropy}};

CREATE TABLE {{.scratch_schema}}.users_userids_this_run{{.entropy}}
  DISTSTYLE KEY
  DISTKEY (domain_userid)
  SORTKEY (domain_userid)
AS(
  SELECT
    a.domain_userid,
    LEAST(a.start_tstamp, b.start_tstamp) AS start_tstamp

  FROM
    {{.scratch_schema}}.sessions_userid_manifest_staged{{.entropy}} a
  LEFT JOIN
    {{.output_schema}}.users_manifest{{.entropy}} b
    ON a.domain_userid = b.domain_userid
);
