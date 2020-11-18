-- Probably just move these into page view events step
  -- Consider potential for duplicate impact. Probably low.

CREATE OR REPLACE TABLE {{.scratch_schema}}.pv_addon_iab{{.entropy}} (
  page_view_id STRING,
  category STRING,
  primary_impact STRING,
  reason STRING,
  spider_or_robot BOOLEAN
);

{{if eq .iab true}}
  INSERT INTO {{.scratch_schema}}.pv_addon_iab{{.entropy}} (
    SELECT

      ev.page_view_id,

      iab.category,
      iab.primary_impact,
      iab.reason,
      iab.spider_or_robot

    FROM {{.scratch_schema}}.events_staged{{.entropy}} AS ev,
    UNNEST(contexts_com_iab_snowplow_spiders_and_robots_1_0_0) AS iab

    WHERE ev.event_name = 'page_view'
  );
{{end}}