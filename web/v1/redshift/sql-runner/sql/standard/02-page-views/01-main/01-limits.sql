-- Create a limit for this run - single row table.
DROP TABLE IF EXISTS {{.scratch_schema}}.pv_run_limits{{.entropy}};

CREATE TABLE {{.scratch_schema}}.pv_run_limits{{.entropy}} AS(
  SELECT
    MIN(collector_tstamp) AS lower_limit,
    MAX(collector_tstamp) AS upper_limit

  FROM
    {{.scratch_schema}}.events_staged{{.entropy}}
);
