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

DROP TABLE IF EXISTS {{.scratch_schema}}.users_this_run{{.entropy}};

CREATE TABLE {{.scratch_schema}}.users_this_run{{.entropy}}
  DISTSTYLE KEY
  DISTKEY (domain_userid)
  SORTKEY (start_tstamp)
AS (
  SELECT
    -- user fields
    a.user_id,
    a.domain_userid,
    a.network_userid,

    b.start_tstamp,
    b.end_tstamp,

    -- engagement fields
    b.page_views,
    b.sessions,

    b.engaged_time_in_s,

    -- first page fields
    a.first_page_title,

    a.first_page_url,

    a.first_page_urlscheme,
    a.first_page_urlhost,
    a.first_page_urlpath,
    a.first_page_urlquery,
    a.first_page_urlfragment,

    c.last_page_title,

    c.last_page_url,

    c.last_page_urlscheme,
    c.last_page_urlhost,
    c.last_page_urlpath,
    c.last_page_urlquery,
    c.last_page_urlfragment,

    -- referrer fields
    a.referrer,

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
    a.mkt_network

  FROM {{.scratch_schema}}.users_aggregates{{.entropy}} AS b

  INNER JOIN {{.scratch_schema}}.users_sessions_this_run{{.entropy}} AS a
    ON a.domain_userid = b.domain_userid
    AND a.start_tstamp = b.start_tstamp

  INNER JOIN {{.scratch_schema}}.users_lasts{{.entropy}} c
    ON b.domain_userid = c.domain_userid
);
