-- 1. Create prod table

CREATE TABLE IF NOT EXISTS {{.output_schema}}.page_views_join{{.entropy}} (
  page_view_id CHAR(36),
  link_clicks INT,
  first_link_target CHAR(2000),
  bounced_page_view BOOLEAN,
  engagement_score FLOAT,
  channel CHAR(255)
);
