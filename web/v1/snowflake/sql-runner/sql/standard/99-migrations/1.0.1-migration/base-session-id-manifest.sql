ALTER TABLE IF EXISTS {{.output_schema}}.base_session_id_manifest{{.entropy}}
 ALTER COLUMN session_id SET DATA TYPE VARCHAR(128);
