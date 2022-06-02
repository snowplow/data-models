-- Extend column lengths to match their correct values
-- Since ALTER TABLE ALTER COLUMN cannot run inside a multiple command statement, we need a file each.
ALTER TABLE {{.output_schema}}.users_manifest{{.entropy}} ALTER COLUMN domain_userid type VARCHAR(128);