ALTER TABLE IF EXISTS {{.scratch_schema}}.events_staged{{.entropy}}
 ALTER COLUMN se_label SET DATA TYPE VARCHAR(4096);