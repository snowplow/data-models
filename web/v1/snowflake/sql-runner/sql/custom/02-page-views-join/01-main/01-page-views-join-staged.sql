/*
   Copyright 2021 Snowplow Analytics Ltd. All rights reserved.

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


CREATE OR REPLACE TABLE {{.scratch_schema}}.page_views_join_staged{{.entropy}}
AS (
  WITH link_clicks AS (
    SELECT
      page_view_id,

      COUNT(event_id) OVER (
        PARTITION BY page_view_id
        ORDER BY derived_tstamp DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
      AS link_clicks,

      FIRST_VALUE(unstruct_event_com_snowplowanalytics_snowplow_link_click_1:targetUrl::varchar) OVER (
        PARTITION BY page_view_id
        ORDER BY derived_tstamp DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
      AS first_link_target

    FROM
      {{.scratch_schema}}.events_staged{{.entropy}}

    WHERE page_view_id IS NOT NULL
      AND event_name = 'link_click'
  )
  SELECT
    pv.page_view_id,
    pv.start_tstamp,

    COALESCE(lc.link_clicks, 0) AS link_clicks,
    lc.first_link_target,

    CASE
      WHEN pv.engaged_time_in_s = 0 THEN TRUE
      ELSE FALSE
    END AS bounced_page_view,

    CASE
      WHEN pv.engaged_time_in_s > 600 THEN (vertical_percentage_scrolled / 100) * 0.3 + 0.7
      ELSE (vertical_percentage_scrolled / 100) * 0.3 + (engaged_time_in_s / 600) * 0.7
    END AS engagement_score,

    CASE
      WHEN pv.refr_medium = 'search'
        AND RLIKE(LOWER(pv.mkt_medium), '.*(cpc|ppc|sem|paidsearch).*')
        OR RLIKE(LOWER(pv.mkt_source), '.*(cpc|ppc|sem|paidsearch).*') THEN 'paidsearch'

      WHEN ILIKE(pv.mkt_medium, '%paidsearch%')
        OR ILIKE(pv.mkt_source, '%paidsearch%') THEN 'paidsearch'

      WHEN RLIKE(LOWER(mkt_source), '.*(adwords|google_paid|googleads).*')
        OR RLIKE(LOWER(mkt_medium), '.*(adwords|google_paid|googleads).*') THEN 'paidsearch'

      WHEN ILIKE(pv.mkt_source, '%google%')
        AND ILIKE(pv.mkt_medium, '%ads%') THEN 'paidsearch'

      WHEN pv.refr_urlhost IN ('www.googleadservices.com','googleads.g.doubleclick.net') THEN 'paidsearch'

      WHEN RLIKE(LOWER(pv.mkt_medium), '.*(cpv|cpa|cpp|content-text|advertising|ads).*') THEN 'advertising'

      WHEN RLIKE(LOWER(pv.mkt_medium), '.*(display|cpm|banner).*') THEN 'display'

      WHEN pv.refr_medium IS NULL
        AND NOT ILIKE(pv.page_url, '%utm_%') THEN 'direct'

      WHEN (LOWER(pv.refr_medium) = 'search' AND pv.mkt_medium IS NULL)
        OR (LOWER(pv.refr_medium) = 'search' AND LOWER(pv.mkt_medium) = 'organic') THEN 'organicsearch'

      WHEN pv.refr_medium = 'social'
        OR RLIKE(LOWER(pv.mkt_source), '^((.*(facebook|linkedin|instagram|insta|slideshare|social|tweet|twitter|youtube|lnkd|pinterest|googleplus|instagram|plus.google.com|quora|reddit|t.co|twitch|viadeo|xing|youtube).*)|(yt|fb|li))$')
        OR RLIKE(LOWER(pv.mkt_medium), '^.*(social|facebook|linkedin|twitter|instagram|tweet).*$') THEN 'social'

      WHEN pv.refr_medium = 'email'
        OR ILIKE(pv.mkt_medium, '_mail') THEN 'email'

      WHEN ILIKE(pv.mkt_medium, 'affiliate') THEN 'affiliate'

      WHEN pv.refr_medium = 'unknown'
        OR ILIKE(pv.mkt_medium, 'referral')
        OR ILIKE(pv.mkt_medium, 'referal') THEN 'referral'

      WHEN pv.refr_medium = 'internal' THEN 'internal'

      ELSE 'others'
    END AS channel

  FROM
    {{.scratch_schema}}.page_views_staged{{.entropy}} AS pv

  LEFT JOIN (SELECT * FROM link_clicks QUALIFY ROW_NUMBER() OVER (PARTITION BY page_view_id ORDER BY page_view_id) = 1) AS lc
    ON lc.page_view_id = pv.page_view_id
);
