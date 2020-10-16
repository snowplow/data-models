-- 1. Create prod table

CREATE TABLE IF NOT EXISTS {{.output_schema}}.pages{{.entropy}} (
  page_urlhost CHAR(36),
  page_views INT,
  link_clicks INT,
  sessions INT,
  users INT,
  engaged_time_in_s INT,
  absolute_time_in_s INT
);
