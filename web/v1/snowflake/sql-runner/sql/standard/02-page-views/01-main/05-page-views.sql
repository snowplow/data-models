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


CREATE OR REPLACE TABLE {{.scratch_schema}}.page_views_this_run{{.entropy}}
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

    ev.page_view_in_session_index,
    MAX(ev.page_view_in_session_index) OVER (PARTITION BY domain_sessionid) AS page_views_in_session,

    -- timestamp fields
    ev.dvce_created_tstamp,
    ev.collector_tstamp,
    ev.derived_tstamp,
    ev.derived_tstamp AS start_tstamp,
    -- only page views with pings will have a row in table t, hence the COALESCE
    COALESCE(t.end_tstamp, ev.derived_tstamp) AS end_tstamp,

    -- where there are no pings, engaged time is 0
    COALESCE(t.engaged_time_in_s, 0) AS engaged_time_in_s,
    TIMEDIFF(second, ev.derived_tstamp, COALESCE(t.end_tstamp, ev.derived_tstamp))  AS absolute_time_in_s,

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

    ev.category,
    ev.primary_impact,
    ev.reason,
    ev.spider_or_robot,

    ev.useragent_family,
    ev.useragent_major,
    ev.useragent_minor,
    ev.useragent_patch,
    ev.useragent_version,
    ev.os_family,
    ev.os_major,
    ev.os_minor,
    ev.os_patch,
    ev.os_patch_minor,
    ev.os_version,
    ev.device_family,

    ev.device_class,
    ev.agent_class,
    ev.agent_name,
    ev.agent_name_version,
    ev.agent_name_version_major,
    ev.agent_version,
    ev.agent_version_major,
    ev.device_brand,
    ev.device_name,
    ev.device_version,
    ev.layout_engine_class,
    ev.layout_engine_name,
    ev.layout_engine_name_version,
    ev.layout_engine_name_version_major,
    ev.layout_engine_version,
    ev.layout_engine_version_major,
    ev.operating_system_class,
    ev.operating_system_name,
    ev.operating_system_name_version,
    ev.operating_system_version

  FROM {{.scratch_schema}}.pv_page_view_events{{.entropy}} AS ev

  LEFT JOIN {{.scratch_schema}}.pv_engaged_time{{.entropy}} AS t
    ON ev.page_view_id = t.page_view_id

  LEFT JOIN {{.scratch_schema}}.pv_scroll_depth{{.entropy}} AS sd
    ON ev.page_view_id = sd.page_view_id
);
