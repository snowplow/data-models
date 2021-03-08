-- Optional contexts, only populated if enabled

-- iab enrichment: set iab variable to true to enable
{{if eq .iab true}}

  CALL {{.output_schema}}.combine_context_versions('contexts_com_iab_snowplow_spiders_and_robots_1');

{{else}}
  CREATE OR REPLACE TABLE {{.scratch_schema}}.contexts_com_iab_snowplow_spiders_and_robots_1{{.entropy}}
  AS(
    SELECT
      -- SELECT NULL returns an int64 column type, cast to ensure correct type.
      CAST(NULL AS STRING) AS event_id,
      CAST(NULL AS STRING) AS page_view_id,
      CAST(NULL AS TIMESTAMP) AS collector_tstamp,
      CAST(NULL AS TIMESTAMP) AS derived_tstamp,
      CAST(NULL AS STRING) AS category,
      CAST(NULL AS STRING) AS primary_impact,
      CAST(NULL AS STRING) AS reason,
      CAST(NULL AS BOOL) AS spider_or_robot
  );

{{end}}

-- ua parser enrichment: set ua_parser variable to true to enable
{{if eq .ua_parser true}}

  CALL {{.output_schema}}.combine_context_versions('contexts_com_snowplowanalytics_snowplow_ua_parser_context_1');

{{else}}
  CREATE OR REPLACE TABLE {{.scratch_schema}}.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1{{.entropy}}
  AS(
    SELECT
      CAST(NULL AS STRING) AS event_id,
      CAST(NULL AS STRING) AS page_view_id,
      CAST(NULL AS TIMESTAMP) AS collector_tstamp,
      CAST(NULL AS TIMESTAMP) AS derived_tstamp,
      CAST(NULL AS STRING) AS useragent_family,
      CAST(NULL AS STRING) AS useragent_major,
      CAST(NULL AS STRING) AS useragent_minor,
      CAST(NULL AS STRING) AS useragent_patch,
      CAST(NULL AS STRING) AS useragent_version,
      CAST(NULL AS STRING) AS os_family,
      CAST(NULL AS STRING) AS os_major,
      CAST(NULL AS STRING) AS os_minor,
      CAST(NULL AS STRING) AS os_patch,
      CAST(NULL AS STRING) AS os_patch_minor,
      CAST(NULL AS STRING) AS os_version,
      CAST(NULL AS STRING) AS device_family
  );

{{end}}

-- yauaa enrichment: set yauaa variable to true to enable
{{if eq .yauaa true}}

  CALL {{.output_schema}}.combine_context_versions('contexts_nl_basjes_yauaa_context_1');

{{else}}

  CREATE OR REPLACE TABLE {{.scratch_schema}}.contexts_nl_basjes_yauaa_context_1{{.entropy}}
  AS(
    SELECT
      CAST(NULL AS STRING) AS event_id,
      CAST(NULL AS STRING) AS page_view_id,
      CAST(NULL AS TIMESTAMP) AS collector_tstamp,
      CAST(NULL AS TIMESTAMP) AS derived_tstamp,
      CAST(NULL AS STRING) AS device_class,
      CAST(NULL AS STRING) AS agent_class,
      CAST(NULL AS STRING) AS agent_name,
      CAST(NULL AS STRING) AS agent_name_version,
      CAST(NULL AS STRING) AS agent_name_version_major,
      CAST(NULL AS STRING) AS agent_version,
      CAST(NULL AS STRING) AS agent_version_major,
      CAST(NULL AS STRING) AS device_brand,
      CAST(NULL AS STRING) AS device_name,
      CAST(NULL AS STRING) AS device_version,
      CAST(NULL AS STRING) AS layout_engine_class,
      CAST(NULL AS STRING) AS layout_engine_name,
      CAST(NULL AS STRING) AS layout_engine_name_version,
      CAST(NULL AS STRING) AS layout_engine_name_version_major,
      CAST(NULL AS STRING) AS layout_engine_version,
      CAST(NULL AS STRING) AS layout_engine_version_major,
      CAST(NULL AS STRING) AS operating_system_class,
      CAST(NULL AS STRING) AS operating_system_name,
      CAST(NULL AS STRING) AS operating_system_name_version,
      CAST(NULL AS STRING) AS operating_system_version
  );

{{end}}
