ALTER TABLE IF EXISTS {{.output_schema}}.users_manifest{{.entropy}}
 ALTER COLUMN domain_userid SET DATA TYPE VARCHAR(128);