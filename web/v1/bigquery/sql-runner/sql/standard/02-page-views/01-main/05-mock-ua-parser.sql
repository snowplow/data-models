CREATE OR REPLACE TABLE {{.scratch_schema}}.pv_addon_ua_parser{{.entropy}} (
  page_view_id STRING,
  useragent_family STRING,
  useragent_major STRING,
  useragent_minor STRING,
  useragent_patch STRING,
  useragent_version STRING,
  os_family STRING,
  os_major STRING,
  os_minor STRING,
  os_patch STRING,
  os_patch_minor STRING,
  os_version STRING,
  device_family STRING
);

{{if eq .ua_parser true}}
  INSERT INTO {{.scratch_schema}}.pv_addon_ua_parser{{.entropy}} (
    SELECT

      ev.page_view_id,

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

    FROM {{.scratch_schema}}.events_staged{{.entropy}} AS ev,
    UNNEST(contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_0) AS ua
  );
{{end}}
