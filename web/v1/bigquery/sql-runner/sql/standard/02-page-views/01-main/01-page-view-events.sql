/*
   Copyright 2020-2021 Snowplow Analytics Ltd. All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

CREATE OR REPLACE TABLE {{.scratch_schema}}.pv_page_view_events{{.entropy}}
AS(
  SELECT
    ev.page_view_id,
    ev.event_id,

    ev.app_id,

    -- user fields
    ev.user_id,
    ev.domain_userid,
    ev.network_userid,

    -- session fields
    ev.domain_sessionid,
    ev.domain_sessionidx,

    -- timestamp fields
    ev.dvce_created_tstamp,
    ev.collector_tstamp,
    ev.derived_tstamp,
    ev.derived_tstamp AS start_tstamp,

    ev.doc_width,
    ev.doc_height,

    ev.page_title,
    ev.page_url,
    ev.page_urlscheme,
    ev.page_urlhost,
    ev.page_urlpath,
    ev.page_urlquery,
    ev.page_urlfragment,

    ev.mkt_medium,
    ev.mkt_source,
    ev.mkt_term,
    ev.mkt_content,
    ev.mkt_campaign,
    ev.mkt_clickid,
    ev.mkt_network,

    ev.page_referrer,
    ev.refr_urlscheme ,
    ev.refr_urlhost,
    ev.refr_urlpath,
    ev.refr_urlquery,
    ev.refr_urlfragment,
    ev.refr_medium,
    ev.refr_source,
    ev.refr_term,

    ev.geo_country,
    ev.geo_region,
    ev.geo_region_name,
    ev.geo_city,
    ev.geo_zipcode,
    ev.geo_latitude,
    ev.geo_longitude,
    ev.geo_timezone ,

    ev.user_ipaddress,

    ev.useragent,

    ev.br_lang,
    ev.br_viewwidth,
    ev.br_viewheight,
    ev.br_colordepth,
    ev.br_renderengine,
    ev.os_timezone,

    -- Optional fields, only populated if enabled.

    -- iab enrichment fields: set iab variable to true to enable
    {{if eq .iab true}}

      ev.contexts_com_iab_snowplow_spiders_and_robots_1_0_0[SAFE_OFFSET(0)].*,

    {{else}}

      -- SELECT NULL returns an int64 column type, cast to ensure correct type.
      CAST(NULL AS STRING) AS category,
      CAST(NULL AS STRING) AS primary_impact,
      CAST(NULL AS STRING) AS reason,
      CAST(NULL AS BOOL) AS spider_or_robot,

    {{end}}

    -- ua parser enrichment fields: set ua_parser variable to true to enable
    {{if eq .ua_parser true}}

      ev.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_0[SAFE_OFFSET(0)].*,

    {{else}}

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
      CAST(NULL AS STRING) AS device_family,

    {{end}}

    -- yauaa enrichment fields: set yauaa variable to true to enable
    {{if eq .yauaa true}}

      ev.contexts_nl_basjes_yauaa_context_1_0_0[SAFE_OFFSET(0)].*,

    {{else}}

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
      CAST(NULL AS STRING) AS operating_system_version,

    {{end}}

    ROW_NUMBER() OVER (PARTITION BY ev.domain_sessionid ORDER BY ev.derived_tstamp) AS page_view_in_session_index

  FROM (
    SELECT
      ARRAY_AGG(e ORDER BY e.derived_tstamp LIMIT 1)[OFFSET(0)] AS ev
      -- order by matters here since derived_tstamp determines parts of model logic

    FROM {{.scratch_schema}}.events_staged{{.entropy}} e
    WHERE e.event_name = 'page_view'
    GROUP BY e.page_view_id
  )

  {{if eq .ua_bot_filter true}}
  -- Move this into subquery?
    WHERE NOT REGEXP_CONTAINS(ev.useragent, '%(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt)%')
  {{end}}
);
