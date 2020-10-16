{{if eq .cleanup_mode "trace"}} SELECT 1; {{else}}
  DROP TABLE IF EXISTS {{.scratch_schema}}.users_aggregates{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.users_lasts{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.users_run_metadata_temp{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.users_metadata_this_run{{.entropy}};
{{end}}

{{if eq .cleanup_mode "debug" "trace"}} SELECT 1; {{else}}
  DROP TABLE IF EXISTS {{.scratch_schema}}.users_userids_this_run{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.users_limits{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.users_this_run{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.users_sessions_this_run{{.entropy}};
{{end}}

{{if eq .ends_run true}}
  DROP TABLE IF EXISTS {{.scratch_schema}}.metadata_run_id{{.entropy}};
{{end}}
