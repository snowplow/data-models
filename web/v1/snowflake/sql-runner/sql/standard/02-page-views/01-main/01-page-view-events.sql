/*
   Copyright 2021-2022 Snowplow Analytics Ltd. All rights reserved.

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
AS (
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

    ev.contexts_com_iab_snowplow_spiders_and_robots_1[0]:category::VARCHAR AS category,
    ev.contexts_com_iab_snowplow_spiders_and_robots_1[0]:primaryImpact::VARCHAR AS primary_impact,
    ev.contexts_com_iab_snowplow_spiders_and_robots_1[0]:reason::VARCHAR AS reason,
    ev.contexts_com_iab_snowplow_spiders_and_robots_1[0]:spiderOrRobot::BOOLEAN AS spider_or_robot,

    {{else}}

    CAST(NULL AS VARCHAR) AS category,
    CAST(NULL AS VARCHAR) AS primary_impact,
    CAST(NULL AS VARCHAR) AS reason,
    CAST(NULL AS BOOLEAN) AS spider_or_robot,

    {{end}}

    -- ua parser enrichment fields: set ua_parser variable to true to enable
    {{if eq .ua_parser true}}

    ev.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0]:useragentFamily::VARCHAR AS useragent_family,
    ev.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0]:useragentMajor::VARCHAR AS useragent_major,
    ev.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0]:useragentMinor::VARCHAR AS useragent_minor,
    ev.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0]:useragentPatch::VARCHAR AS useragent_patch,
    ev.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0]:useragentVersion::VARCHAR AS useragent_version,
    ev.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0]:osFamily::VARCHAR AS os_family,
    ev.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0]:osMajor::VARCHAR AS os_major,
    ev.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0]:osMinor::VARCHAR AS os_minor,
    ev.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0]:osPatch::VARCHAR AS os_patch,
    ev.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0]:osPatchMinor::VARCHAR AS os_patch_minor,
    ev.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0]:osVersion::VARCHAR AS os_version,
    ev.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0]:deviceFamily::VARCHAR AS device_family,

    {{else}}

    CAST(NULL AS VARCHAR) AS useragent_family,
    CAST(NULL AS VARCHAR) AS useragent_major,
    CAST(NULL AS VARCHAR) AS useragent_minor,
    CAST(NULL AS VARCHAR) AS useragent_patch,
    CAST(NULL AS VARCHAR) AS useragent_version,
    CAST(NULL AS VARCHAR) AS os_family,
    CAST(NULL AS VARCHAR) AS os_major,
    CAST(NULL AS VARCHAR) AS os_minor,
    CAST(NULL AS VARCHAR) AS os_patch,
    CAST(NULL AS VARCHAR) AS os_patch_minor,
    CAST(NULL AS VARCHAR) AS os_version,
    CAST(NULL AS VARCHAR) AS device_family,

    {{end}}

    -- yauaa enrichment fields: set yauaa variable to true to enable
    {{if eq .yauaa true}}

    ev.contexts_nl_basjes_yauaa_context_1[0]:deviceClass::VARCHAR AS device_class,
    ev.contexts_nl_basjes_yauaa_context_1[0]:agentClass::VARCHAR AS agent_class,
    ev.contexts_nl_basjes_yauaa_context_1[0]:agentName::VARCHAR AS agent_name,
    ev.contexts_nl_basjes_yauaa_context_1[0]:agentNameVersion::VARCHAR AS agent_name_version,
    ev.contexts_nl_basjes_yauaa_context_1[0]:agentNameVersionMajor::VARCHAR AS agent_name_version_major,
    ev.contexts_nl_basjes_yauaa_context_1[0]:agentVersion::VARCHAR AS agent_version,
    ev.contexts_nl_basjes_yauaa_context_1[0]:agentVersionMajor::VARCHAR AS agent_version_major,
    ev.contexts_nl_basjes_yauaa_context_1[0]:deviceBrand::VARCHAR AS device_brand,
    ev.contexts_nl_basjes_yauaa_context_1[0]:deviceName::VARCHAR AS device_name,
    ev.contexts_nl_basjes_yauaa_context_1[0]:deviceVersion::VARCHAR AS device_version,
    ev.contexts_nl_basjes_yauaa_context_1[0]:layoutEngineClass::VARCHAR AS layout_engine_class,
    ev.contexts_nl_basjes_yauaa_context_1[0]:layoutEngineName::VARCHAR AS layout_engine_name,
    ev.contexts_nl_basjes_yauaa_context_1[0]:layoutEngineNameVersion::VARCHAR AS layout_engine_name_version,
    ev.contexts_nl_basjes_yauaa_context_1[0]:layoutEngineNameVersionMajor::VARCHAR AS layout_engine_name_version_major,
    ev.contexts_nl_basjes_yauaa_context_1[0]:layoutEngineVersion::VARCHAR AS layout_engine_version,
    ev.contexts_nl_basjes_yauaa_context_1[0]:layoutEngineVersionMajor::VARCHAR AS layout_engine_version_major,
    ev.contexts_nl_basjes_yauaa_context_1[0]:operatingSystemClass::VARCHAR AS operating_system_class,
    ev.contexts_nl_basjes_yauaa_context_1[0]:operatingSystemName::VARCHAR AS operating_system_name,
    ev.contexts_nl_basjes_yauaa_context_1[0]:operatingSystemNameVersion::VARCHAR AS operating_system_name_version,
    ev.contexts_nl_basjes_yauaa_context_1[0]:operatingSystemVersion::VARCHAR AS operating_system_version,

    {{else}}

    CAST(NULL AS VARCHAR) AS device_class,
    CAST(NULL AS VARCHAR) AS agent_class,
    CAST(NULL AS VARCHAR) AS agent_name,
    CAST(NULL AS VARCHAR) AS agent_name_version,
    CAST(NULL AS VARCHAR) AS agent_name_version_major,
    CAST(NULL AS VARCHAR) AS agent_version,
    CAST(NULL AS VARCHAR) AS agent_version_major,
    CAST(NULL AS VARCHAR) AS device_brand,
    CAST(NULL AS VARCHAR) AS device_name,
    CAST(NULL AS VARCHAR) AS device_version,
    CAST(NULL AS VARCHAR) AS layout_engine_class,
    CAST(NULL AS VARCHAR) AS layout_engine_name,
    CAST(NULL AS VARCHAR) AS layout_engine_name_version,
    CAST(NULL AS VARCHAR) AS layout_engine_name_version_major,
    CAST(NULL AS VARCHAR) AS layout_engine_version,
    CAST(NULL AS VARCHAR) AS layout_engine_version_major,
    CAST(NULL AS VARCHAR) AS operating_system_class,
    CAST(NULL AS VARCHAR) AS operating_system_name,
    CAST(NULL AS VARCHAR) AS operating_system_name_version,
    CAST(NULL AS VARCHAR) AS operating_system_version,

    {{end}}

    ROW_NUMBER() OVER (PARTITION BY ev.domain_sessionid
                       ORDER BY ev.derived_tstamp) AS page_view_in_session_index

  FROM
    {{.scratch_schema}}.events_staged{{.entropy}} AS ev

  WHERE ev.event_name = 'page_view'
    AND ev.page_view_id IS NOT NULL

  {{if eq .ua_bot_filter true}}
    AND NOT RLIKE(ev.useragent, '.*(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt).*')
  {{end}}
);
