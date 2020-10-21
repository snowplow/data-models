-- Create upsert limit
DROP TABLE IF EXISTS {{.scratch_schema}}.pv_upsert_limit{{.entropy}};

CREATE TABLE {{.scratch_schema}}.pv_upsert_limit{{.entropy}} AS (
  SELECT
    DATEADD(DAY, -{{or .upsert_lookback 30}}, min(start_tstamp)) AS lower_limit
  FROM {{.scratch_schema}}.page_views_this_run{{.entropy}}
);

BEGIN;

  {{if ne (or .skip_derived false) true}}
    -- Commit production table
    DELETE FROM {{.output_schema}}.page_views{{.entropy}}
      WHERE page_view_id IN (SELECT page_view_id FROM {{.scratch_schema}}.page_views_this_run{{.entropy}})
        AND start_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.pv_upsert_limit{{.entropy}});

    INSERT INTO {{.output_schema}}.page_views{{.entropy}}
      (SELECT * FROM {{.scratch_schema}}.page_views_this_run{{.entropy}});
  {{end}}

  {{if eq .stage_next true}}
    -- Commit staging table if enabled
    DELETE FROM {{.scratch_schema}}.page_views_staged{{.entropy}}
      WHERE page_view_id IN (SELECT page_view_id FROM {{.scratch_schema}}.page_views_this_run{{.entropy}});

    INSERT INTO {{.scratch_schema}}.page_views_staged{{.entropy}}
      (SELECT * FROM {{.scratch_schema}}.page_views_this_run{{.entropy}});
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
    FROM {{.scratch_schema}}.pv_metadata_this_run{{.entropy}}
  );

END;