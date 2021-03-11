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

CREATE OR REPLACE TABLE {{.scratch_schema}}.page_views_this_run{{.entropy}}
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

    ev.page_view_in_session_index,
    MAX(ev.page_view_in_session_index) OVER (PARTITION BY domain_sessionid) AS page_views_in_session,

    -- timestamp fields
    ev.dvce_created_tstamp,
    ev.collector_tstamp,
    ev.derived_tstamp,
    ev.derived_tstamp AS start_tstamp,
    COALESCE(t.end_tstamp, ev.derived_tstamp) AS end_tstamp, -- only page views with pings will have a row in table t

    COALESCE(t.engaged_time_in_s, 0) AS engaged_time_in_s, -- where there are no pings, engaged time is 0.
    TIMESTAMP_DIFF(COALESCE(t.end_tstamp, ev.derived_tstamp), ev.derived_tstamp, SECOND)  AS absolute_time_in_s,

    sd.hmax AS horizontal_pixels_scrolled,
    sd.vmax AS vertical_pixels_scrolled,

    sd.relative_hmax AS horizontal_percentage_scrolled,
    sd.relative_vmax AS vertical_percentage_scrolled,

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
    ev.refr_urlscheme,
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
    ev.geo_timezone,

    ev.user_ipaddress,

    ev.useragent,

    ev.br_lang,
    ev.br_viewwidth,
    ev.br_viewheight,
    ev.br_colordepth,
    ev.br_renderengine,

    ev.os_timezone,

    -- Optional contexts, only populated if enabled

    -- iab enrichment fields: set iab variable to true to enable
    iab.category,
    iab.primary_impact,
    iab.reason,
    iab.spider_or_robot,

    -- ua parser enrichment fields: set ua_parser variable to true to enable
    uap.useragent_family,
    uap.useragent_major,
    uap.useragent_minor,
    uap.useragent_patch,
    uap.useragent_version,
    uap.os_family,
    uap.os_major,
    uap.os_minor,
    uap.os_patch,
    uap.os_patch_minor,
    uap.os_version,
    uap.device_family,

    -- yauaa enrichment fields: set yauaa variable to true to enable
    yauaa.device_class,
    yauaa.agent_class,
    yauaa.agent_name,
    yauaa.agent_name_version,
    yauaa.agent_name_version_major,
    yauaa.agent_version,
    yauaa.agent_version_major,
    yauaa.device_brand,
    yauaa.device_name,
    yauaa.device_version,
    yauaa.layout_engine_class,
    yauaa.layout_engine_name,
    yauaa.layout_engine_name_version,
    yauaa.layout_engine_name_version_major,
    yauaa.layout_engine_version,
    yauaa.layout_engine_version_major,
    yauaa.operating_system_class,
    yauaa.operating_system_name,
    yauaa.operating_system_name_version,
    yauaa.operating_system_version

  FROM {{.scratch_schema}}.pv_page_view_events{{.entropy}} ev

  LEFT JOIN {{.scratch_schema}}.pv_engaged_time{{.entropy}} t
  ON ev.page_view_id = t.page_view_id

  LEFT JOIN {{.scratch_schema}}.pv_scroll_depth{{.entropy}} sd
  ON ev.page_view_id = sd.page_view_id

  LEFT JOIN {{.scratch_schema}}.contexts_com_iab_snowplow_spiders_and_robots_1{{.entropy}} iab
  ON ev.page_view_id = iab.page_view_id
  AND ev.event_id = iab.event_id

  LEFT JOIN {{.scratch_schema}}.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1{{.entropy}} uap
  ON ev.page_view_id = uap.page_view_id
  AND ev.event_id = uap.event_id

  LEFT JOIN {{.scratch_schema}}.contexts_nl_basjes_yauaa_context_1{{.entropy}} yauaa
  ON ev.page_view_id = yauaa.page_view_id
  AND ev.event_id = yauaa.event_id
);
