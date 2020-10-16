BEGIN;

  {{if ne (or .skip_derived false) true}}
    -- Commit production table
    DELETE FROM {{.output_schema}}.users{{.entropy}}
      WHERE domain_userid IN (SELECT domain_userid FROM {{.scratch_schema}}.users_this_run{{.entropy}})
        AND start_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.users_limits{{.entropy}});

    INSERT INTO {{.output_schema}}.users{{.entropy}}
      (SELECT * FROM {{.scratch_schema}}.users_this_run{{.entropy}});
  {{end}}

  -- Commit metadata
  INSERT INTO {{.output_schema}}.web_model_run_metadata{{.entropy}} (
    SELECT
      run_id,
      model_version,
      module_name,
      step_name,
      run_start_tstamp,
      GETDATE() AS run_end_tstamp,
      rows_this_run,
      distinct_key,
      distinct_key_count,
      time_key,
      min_time_key,
      max_time_key,
      duplicate_rows_removed,
      distinct_keys_removed
    FROM {{.scratch_schema}}.users_metadata_this_run{{.entropy}}
  );

END;
