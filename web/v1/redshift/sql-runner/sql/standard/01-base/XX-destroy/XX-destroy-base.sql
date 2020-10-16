DROP TABLE IF EXISTS {{.output_schema}}.base_event_id_manifest{{.entropy}};
DROP TABLE IF EXISTS {{.output_schema}}.base_session_id_manifest{{.entropy}};
DROP TABLE IF EXISTS {{.scratch_schema}}.events_staged{{.entropy}};
