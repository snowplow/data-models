-- Permanent metadata table
CREATE TABLE IF NOT EXISTS {{.output_schema}}.web_model_run_metadata{{.entropy}} (
  run_id TIMESTAMP,
  model_version VARCHAR(64),
  module_name VARCHAR(64),
  step_name VARCHAR(64),
  run_start_tstamp TIMESTAMP,
  run_end_tstamp TIMESTAMP,
  rows_this_run INT,
  distinct_key VARCHAR(64),
  distinct_key_count INT,
  time_key VARCHAR(64),
  min_time_key TIMESTAMP,
  max_time_key TIMESTAMP,
  duplicate_rows_removed INT,
  distinct_keys_removed INT
);

-- A table storing an identifier for this run of a model - used to identify runs of the model across multiple modules/steps (eg. base, page views share this id per run)
CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.metadata_run_id{{.entropy}} (
    run_id TIMESTAMP
);

TRUNCATE {{.scratch_schema}}.metadata_run_id{{.entropy}};

INSERT INTO {{.scratch_schema}}.metadata_run_id{{.entropy}} (
  SELECT
    GETDATE()
);
