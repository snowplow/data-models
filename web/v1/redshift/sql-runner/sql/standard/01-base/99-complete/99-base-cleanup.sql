{{if eq .cleanup_mode "trace"}} SELECT 1; {{else}}
  DROP TABLE IF EXISTS {{.scratch_schema}}.base_new_events_limits{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.base_sessions_to_process{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.base_sessions_to_include{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.base_metadata_this_run{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.base_run_metadata_temp{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.base_run_dupe_metadata_temp{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.base_session_id_run_manifest{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.base_events_this_run_tmp{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.base_event_ids_this_run{{.entropy}};
{{end}}

{{if eq .cleanup_mode "debug" "trace"}} SELECT 1; {{else}}
  DROP TABLE IF EXISTS {{.scratch_schema}}.base_run_manifest{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.events_this_run{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.base_duplicates_this_run{{.entropy}};
  DROP TABLE IF EXISTS {{.scratch_schema}}.base_run_limits{{.entropy}};
{{end}}

{{if eq .ends_run true}}
  DROP TABLE IF EXISTS {{.scratch_schema}}.metadata_run_id{{.entropy}};
{{end}}
