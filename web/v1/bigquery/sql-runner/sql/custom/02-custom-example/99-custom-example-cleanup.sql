-- We can run this cleanup straight after the commit step if we like, as long as no subsequent logic depends on it.

DROP TABLE IF EXISTS {{.scratch_schema}}.link_clicks{{.entropy}};
DROP TABLE IF EXISTS {{.scratch_schema}}.channel_engagement_staged{{.entropy}};
