/*
   Copyright 2020 Snowplow Analytics Ltd. All rights reserved.

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

DROP TABLE IF EXISTS {{.scratch_schema}}.sessions_this_run{{.entropy}};

CREATE TABLE {{.scratch_schema}}.sessions_this_run{{.entropy}}
  DISTSTYLE KEY
  DISTKEY (domain_sessionid)
  SORTKEY (start_tstamp)
AS (
  SELECT

    -- app ID
    a.app_id,

    -- session fields
    a.domain_sessionid,
    a.domain_sessionidx,

    b.start_tstamp,
    b.end_tstamp,

    -- user fields
    a.user_id,
    a.domain_userid,
    a.network_userid,

    -- engagement fields
    b.page_views,
    b.engaged_time_in_s,
    DATEDIFF(second, b.start_tstamp, b.end_tstamp)  AS absolute_time_in_s,

    -- first page fields
    a.page_title AS first_page_title,

    a.page_url AS first_page_url,

    a.page_urlscheme AS first_page_urlscheme,
    a.page_urlhost AS first_page_urlhost,
    a.page_urlpath AS first_page_urlpath,
    a.page_urlquery AS first_page_urlquery,
    a.page_urlfragment AS first_page_urlfragment,

    c.last_page_title,

    c.last_page_url,

    c.last_page_urlscheme,
    c.last_page_urlhost,
    c.last_page_urlpath,
    c.last_page_urlquery,
    c.last_page_urlfragment,

    -- referrer fields
    a.page_referrer AS referrer,

    a.refr_urlscheme,
    a.refr_urlhost,
    a.refr_urlpath,
    a.refr_urlquery,
    a.refr_urlfragment,

    a.refr_medium,
    a.refr_source,
    a.refr_term,

    -- marketing fields
    a.mkt_medium,
    a.mkt_source,
    a.mkt_term,
    a.mkt_content,
    a.mkt_campaign,
    a.mkt_clickid,
    a.mkt_network,

    -- geo fields
    a.geo_country,
    a.geo_region,
    a.geo_region_name,
    a.geo_city,
    a.geo_zipcode,
    a.geo_latitude,
    a.geo_longitude,
    a.geo_timezone,

    -- IP address
    a.user_ipaddress,

    -- user agent
    a.useragent,

    a.br_renderengine,
    a.br_lang,

    a.os_timezone,

    -- Optional fields, only populated if in the page views module.

    -- iab enrichment fields
    a.category,
    a.primary_impact,
    a.reason,
    a.spider_or_robot,

    -- ua parser enrichment fields
    a.useragent_family,
    a.useragent_major,
    a.useragent_minor,
    a.useragent_patch,
    a.useragent_version,
    a.os_family,
    a.os_major,
    a.os_minor,
    a.os_patch,
    a.os_patch_minor,
    a.os_version,
    a.device_family,

    -- yauaa enrichment fields
    a.device_class,
    a.agent_class,
    a.agent_name,
    a.agent_name_version,
    a.agent_name_version_major,
    a.agent_version,
    a.agent_version_major,
    a.device_brand,
    a.device_name,
    a.device_version,
    a.layout_engine_class,
    a.layout_engine_name,
    a.layout_engine_name_version,
    a.layout_engine_name_version_major,
    a.layout_engine_version,
    a.layout_engine_version_major,
    a.operating_system_class,
    a.operating_system_name,
    a.operating_system_name_version,
    a.operating_system_version

  FROM {{.scratch_schema}}.sessions_aggregates{{.entropy}} AS b

  INNER JOIN {{.scratch_schema}}.page_views_staged{{.entropy}} AS a
    ON a.domain_sessionid = b.domain_sessionid
    AND a.start_tstamp = b.start_tstamp
    AND a.page_view_in_session_index = 1

  INNER JOIN {{.scratch_schema}}.sessions_lasts{{.entropy}} c
    ON b.domain_sessionid = c.domain_sessionid
);
