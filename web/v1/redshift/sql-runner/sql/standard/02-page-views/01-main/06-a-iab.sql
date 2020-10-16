DROP TABLE IF EXISTS {{.scratch_schema}}.pv_addon_iab{{.entropy}};

CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.pv_addon_iab{{.entropy}} (
  page_view_id CHAR(36),
  category VARCHAR,
  primary_impact VARCHAR,
  reason VARCHAR,
  spider_or_robot BOOLEAN
)
DISTSTYLE KEY
DISTKEY (page_view_id)
SORTKEY (page_view_id);

{{if eq .iab true}}
  INSERT INTO {{.scratch_schema}}.pv_addon_iab{{.entropy}} (
    SELECT

      pv.page_view_id,

      iab.category,
      iab.primary_impact,
      iab.reason,
      iab.spider_or_robot

    FROM {{.input_schema}}.com_iab_snowplow_spiders_and_robots_1 iab

    INNER JOIN {{.scratch_schema}}.pv_page_view_events{{.entropy}} pv
      ON iab.root_id = pv.event_id
      AND iab.root_tstamp = pv.collector_tstamp

    WHERE iab.root_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.pv_run_limits{{.entropy}})
      AND iab.root_tstamp <= (SELECT upper_limit FROM {{.scratch_schema}}.pv_run_limits{{.entropy}})
  );
{{end}}
