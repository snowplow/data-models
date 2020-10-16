-- Create a new limit based on this data
DROP TABLE IF EXISTS {{.scratch_schema}}.base_run_limits{{.entropy}};

CREATE TABLE {{.scratch_schema}}.base_run_limits{{.entropy}} AS(
  SELECT
    MIN(min_tstamp) AS lower_limit,
    (SELECT upper_limit FROM {{.scratch_schema}}.base_new_events_limits{{.entropy}}) AS upper_limit

  FROM
    {{.scratch_schema}}.base_sessions_to_include{{.entropy}}
);
