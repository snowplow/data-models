-- 1. Create prod table

CREATE TABLE IF NOT EXISTS {{.output_schema}}.session_goals{{.entropy}} (
  session_id CHAR(36),
  has_started_registration BOOLEAN,
  has_completed_registration BOOLEAN,
  has_used_search BOOLEAN,
  has_viewed_products BOOLEAN,
  has_completed_goals BOOLEAN
);
