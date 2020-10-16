DROP TABLE IF EXISTS {{.scratch_schema}}.pv_addon_yauaa{{.entropy}};

CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.pv_addon_yauaa{{.entropy}} (
  page_view_id CHAR(36),
  device_class VARCHAR,
  agent_class VARCHAR,
  agent_name VARCHAR,
  agent_name_version VARCHAR,
  agent_name_version_major VARCHAR,
  agent_version VARCHAR,
  agent_version_major VARCHAR,
  device_brand VARCHAR,
  device_name VARCHAR,
  device_version VARCHAR,
  layout_engine_class VARCHAR,
  layout_engine_name VARCHAR,
  layout_engine_name_version VARCHAR,
  layout_engine_name_version_major VARCHAR,
  layout_engine_version VARCHAR,
  layout_engine_version_major VARCHAR,
  operating_system_class VARCHAR,
  operating_system_name VARCHAR,
  operating_system_name_version VARCHAR,
  operating_system_version VARCHAR
)
DISTSTYLE KEY
DISTKEY (page_view_id)
SORTKEY (page_view_id);

{{if eq .yauaa true}}
  INSERT INTO {{.scratch_schema}}.pv_addon_yauaa{{.entropy}} (
    SELECT

      pv.page_view_id,

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

    FROM {{.input_schema}}.nl_basjes_yauaa_context_1 ya

    INNER JOIN {{.scratch_schema}}.pv_page_view_events{{.entropy}} pv
      ON ya.root_id = pv.event_id
      AND ya.root_tstamp = pv.collector_tstamp

    WHERE ya.root_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.pv_run_limits{{.entropy}})
      AND ya.root_tstamp <= (SELECT upper_limit FROM {{.scratch_schema}}.pv_run_limits{{.entropy}})
  );
{{end}}
