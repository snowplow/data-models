-- 1. Create prod table

/* This step is optional. The {{.output_schema}}.commit_table() procedure will create the table if it doesn't exist.
  Should clustering be required, table should be created manually. */

CREATE TABLE IF NOT EXISTS {{.output_schema}}.page_views_join{{.entropy}} (
  page_view_id STRING,
  start_tstamp TIMESTAMP,
  link_clicks INT64,
  first_link_target STRING,
  bounced_page_view BOOLEAN,
  engagement_score FLOAT64,
  channel STRING
);
