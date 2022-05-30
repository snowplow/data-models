-- Change sortkey encodings to RAW
ALTER TABLE {{.output_schema}}.page_views{{.entropy}} ALTER COLUMN start_tstamp ENCODE RAW;

ALTER TABLE {{.scratch_schema}}.page_views_staged{{.entropy}} ALTER COLUMN start_tstamp ENCODE RAW;

ALTER TABLE {{.output_schema}}.sessions{{.entropy}} ALTER COLUMN start_tstamp ENCODE RAW;

ALTER TABLE {{.output_schema}}.users{{.entropy}} ALTER COLUMN start_tstamp ENCODE RAW;