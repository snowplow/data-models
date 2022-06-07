-- Extend column lengths to match their correct values
-- Since ALTER TABLE ALTER COLUMN cannot run inside a multiple command statement, we need a file each.
ALTER TABLE {{.output_schema}}.base_session_id_manifest{{.entropy}}  ALTER COLUMN session_id type VARCHAR(128);
