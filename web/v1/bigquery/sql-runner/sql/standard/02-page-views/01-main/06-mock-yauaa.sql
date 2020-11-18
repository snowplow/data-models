CREATE OR REPLACE TABLE {{.scratch_schema}}.pv_addon_yauaa{{.entropy}} (
  page_view_id STRING,
  device_class STRING,
  agent_class STRING,
  agent_name STRING,
  agent_name_version STRING,
  agent_name_version_major STRING,
  agent_version STRING,
  agent_version_major STRING,
  device_brand STRING,
  device_name STRING,
  device_version STRING,
  layout_engine_class STRING,
  layout_engine_name STRING,
  layout_engine_name_version STRING,
  layout_engine_name_version_major STRING,
  layout_engine_version STRING,
  layout_engine_version_major STRING,
  operating_system_class STRING,
  operating_system_name STRING,
  operating_system_name_version STRING,
  operating_system_version STRING
);

{{if eq .yauaa true}}
  INSERT INTO {{.scratch_schema}}.pv_addon_yauaa{{.entropy}} (
    SELECT

      ev.page_view_id,

      ya.device_class,
      ya.agent_class,
      ya.agent_name,
      ya.agent_name_version,
      ya.agent_name_version_major,
      ya.agent_version,
      ya.agent_version_major,
      ya.device_brand,
      ya.device_name,
      ya.device_version,
      ya.layout_engine_class,
      ya.layout_engine_name,
      ya.layout_engine_name_version,
      ya.layout_engine_name_version_major,
      ya.layout_engine_version,
      ya.layout_engine_version_major,
      ya.operating_system_class,
      ya.operating_system_name,
      ya.operating_system_name_version,
      ya.operating_system_version

    FROM {{.scratch_schema}}.events_staged{{.entropy}} AS ev,
    UNNEST(contexts_nl_basjes_yauaa_context_1_0_0) AS ya
  );
{{end}}
