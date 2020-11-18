CREATE OR REPLACE TABLE {{.scratch_schema}}.users_sessions_this_run{{.entropy}}
AS(
  SELECT
    a.*
  FROM {{.output_schema}}.sessions{{.entropy}} a
);
