BEGIN;

  {{if eq .stage_next true}}
    -- Commit staging if enabled
    DELETE
      FROM {{.scratch_schema}}.events_staged{{.entropy}}
      WHERE event_id IN (SELECT event_id FROM {{.scratch_schema}}.events_this_run{{.entropy}});

    INSERT INTO {{.scratch_schema}}.events_staged{{.entropy}} (
      SELECT * FROM {{.scratch_schema}}.events_this_run{{.entropy}}
    );
  {{end}}

  -- Commit metadata
  INSERT INTO {{.output_schema}}.web_model_run_metadata{{.entropy}} (
    SELECT
      run_id,
      model_version,
      module_name,
      step_name,
      run_start_tstamp,
      run_end_tstamp,
      rows_this_run,
      distinct_key,
      distinct_key_count,
      time_key,
      min_time_key,
      max_time_key,
      duplicate_rows_removed,
      distinct_keys_removed
    FROM {{.scratch_schema}}.base_metadata_this_run{{.entropy}}
  );

END;
