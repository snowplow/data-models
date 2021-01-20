-- 2. Aggregate with a drop and recompute logic

CREATE OR REPLACE TABLE {{.scratch_schema}}.page_views_join_staged{{.entropy}} AS(

  --  using events_staged for other event type
  WITH link_clicks AS (
    SELECT
      ev.page_view_id,

      COUNT(ev.event_id)
        OVER(PARTITION BY ev.page_view_id
        ORDER BY ev.derived_tstamp desc
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        AS link_clicks,

      FIRST_VALUE(ev.unstruct_event_com_snowplowanalytics_snowplow_link_click_1_0_1.target_url)
        OVER(PARTITION BY ev.page_view_id
        ORDER BY ev.derived_tstamp desc
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        AS first_link_target

    FROM {{.scratch_schema}}.events_staged{{.entropy}} ev
  ),

  --  using page_views_staged so needs to run after page views
  engagement AS (

    SELECT
      page_view_id,
      start_tstamp,

      CASE
        WHEN engaged_time_in_s = 0 THEN TRUE
        ELSE FALSE
      END AS bounced_page_view,

      (vertical_percentage_scrolled / 100) * 0.3 + (engaged_time_in_s / 600) * 0.7 AS engagement_score

    FROM {{.scratch_schema}}.page_views_staged{{.entropy}}
  )

  SELECT
    ev.page_view_id,
    eng.start_tstamp,

    link_clicks,
    first_link_target,

    bounced_page_view,
    engagement_score,

    CASE
      WHEN ev.refr_medium = 'search'
       AND (REGEXP_CONTAINS(lower(ev.mkt_medium), '%(cpc|ppc|sem|paidsearch)%')
         OR REGEXP_CONTAINS(lower(ev.mkt_source), '%(cpc|ppc|sem|paidsearch)%')) THEN 'paidsearch'
      WHEN lower(ev.mkt_medium) LIKE '%paidsearch%'
       OR lower(ev.mkt_source) LIKE '%paidsearch%' THEN 'paidsearch'
      WHEN REGEXP_CONTAINS(lower(ev.mkt_source), '%(adwords|google_paid|googleads)%')
       OR REGEXP_CONTAINS(lower(ev.mkt_medium), '%(adwords|google_paid|googleads)%') THEN 'paidsearch'
      WHEN lower(ev.mkt_source) LIKE '%google%'

       AND lower(ev.mkt_medium) LIKE '%ads%' THEN 'paidsearch'
      WHEN ev.refr_urlhost in ('www.googleadservices.com','googleads.g.doubleclick.net') then 'paidsearch'

      WHEN REGEXP_CONTAINS(lower(ev.mkt_medium), '%(cpv|cpa|cpp|content-text|advertising|ads)%') THEN 'advertising'
      WHEN REGEXP_CONTAINS(lower(ev.mkt_medium), '%(display|cpm|banner)%') THEN 'display'

      WHEN ev.refr_medium IS NULL AND ev.page_url NOT LIKE '%utm_%' THEN 'direct'
      WHEN (LOWER(ev.refr_medium) = 'search' AND ev.mkt_medium IS NULL)
       OR (LOWER(ev.refr_medium) = 'search' AND LOWER(ev.mkt_medium) = 'organic') THEN 'organicsearch'
      WHEN ev.refr_medium = 'social'
       OR REGEXP_CONTAINS(LOWER(ev.mkt_source),'^((.*(facebook|linkedin|instagram|insta|slideshare|social|tweet|twitter|youtube|lnkd|pinterest|googleplus|instagram|plus.google.com|quora|reddit|t.co|twitch|viadeo|xing|youtube).*)|(yt|fb|li))$')
       OR REGEXP_CONTAINS(LOWER(ev.mkt_medium),'^(.*)(social|facebook|linkedin|twitter|instagram|tweet)(.*)$') THEN 'social'
      WHEN ev.refr_medium = 'email'
       OR lower(ev.mkt_medium) LIKE '_mail' THEN 'email'
      WHEN lower(ev.mkt_medium) LIKE 'affiliate' THEN 'affiliate'
      WHEN ev.refr_medium = 'unknown' or lower(ev.mkt_medium) LIKE 'referral' OR lower(ev.mkt_medium) LIKE 'referal' THEN 'referral'
      WHEN ev.refr_medium = 'internal' then 'internal'
      ELSE 'others'
    END AS channel

  FROM {{.scratch_schema}}.events_staged{{.entropy}} ev
  LEFT JOIN link_clicks lc
    ON lc.page_view_id = ev.page_view_id

  LEFT JOIN engagement eng
    ON eng.page_view_id = ev.page_view_id

  WHERE event_name = 'page_view'
  );
