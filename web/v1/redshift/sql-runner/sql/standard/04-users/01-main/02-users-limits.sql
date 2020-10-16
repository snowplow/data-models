-- Create a limit for this run - single value table.
DROP TABLE IF EXISTS {{.scratch_schema}}.users_limits{{.entropy}};

CREATE TABLE {{.scratch_schema}}.users_limits{{.entropy}} AS(
  SELECT
    MIN(start_tstamp) AS lower_limit,
    MAX(start_tstamp) AS upper_limit

  FROM
    {{.scratch_schema}}.users_userids_this_run{{.entropy}}
);
