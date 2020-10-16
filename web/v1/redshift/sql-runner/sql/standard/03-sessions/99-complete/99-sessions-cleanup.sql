{{if eq .cleanup_mode "trace"}} SELECT 1; {{else}}
  DROP TABLE IF EXISTS {{.scratch_schema}}.sessions_aggregates{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.sessions_lasts{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.sessions_run_metadata_temp{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.sessions_metadata_this_run{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.sessions_userid_manifest_this_run{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.sessions_upsert_limit{{.entropy}};
{{end}}

{{if eq .cleanup_mode "debug" "trace"}} SELECT 1; {{else}}
  DROP TABLE IF EXISTS {{.scratch_schema}}.sessions_this_run{{.entropy}};
{{end}}

{{if eq .ends_run true}}
  DROP TABLE IF EXISTS {{.scratch_schema}}.metadata_run_id{{.entropy}};
{{end}}
