DROP TABLE IF EXISTS {{.scratch_schema}}.pv_addon_ua_parser{{.entropy}};

CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.pv_addon_ua_parser{{.entropy}} (
  page_view_id CHAR(36),
  useragent_family VARCHAR,
  useragent_major VARCHAR,
  useragent_minor VARCHAR,
  useragent_patch VARCHAR,
  useragent_version VARCHAR,
  os_family VARCHAR,
  os_major VARCHAR,
  os_minor VARCHAR,
  os_patch VARCHAR,
  os_patch_minor VARCHAR,
  os_version VARCHAR,
  device_family VARCHAR
)
DISTSTYLE KEY
DISTKEY (page_view_id)
SORTKEY (page_view_id);

{{if eq .ua_parser true}}
  INSERT INTO {{.scratch_schema}}.pv_addon_ua_parser{{.entropy}} (
    SELECT

      pv.page_view_id,

      ua.useragent_family,
      ua.useragent_major,
      ua.useragent_minor,
      ua.useragent_patch,
      ua.useragent_version,
      ua.os_family,
      ua.os_major,
      ua.os_minor,
      ua.os_patch,
      ua.os_patch_minor,
      ua.os_version,
      ua.device_family

    FROM {{.input_schema}}.com_snowplowanalytics_snowplow_ua_parser_context_1 AS ua

    INNER JOIN {{.scratch_schema}}.pv_page_view_events{{.entropy}} pv
      ON ua.root_id = pv.event_id
      AND ua.root_tstamp = pv.collector_tstamp

    WHERE ua.root_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.pv_run_limits{{.entropy}})
      AND ua.root_tstamp <= (SELECT upper_limit FROM {{.scratch_schema}}.pv_run_limits{{.entropy}})
  );
{{end}}
