-- 2. Aggregate with a drop and recompute logic

DROP TABLE IF EXISTS {{.scratch_schema}}.page_views_join_staged{{.entropy}};

CREATE TABLE {{.scratch_schema}}.page_views_join_staged{{.entropy}} AS(

  --  using events_staged for other event type
  WITH link_clicks AS (
    SELECT
      ev.page_view_id,

      COUNT(ev.event_id)
        OVER(PARTITION BY ev.page_view_id
        ORDER BY ev.derived_tstamp desc
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        AS link_clicks,

      FIRST_VALUE(lc.target_url)
        OVER(PARTITION BY ev.page_view_id
        ORDER BY ev.derived_tstamp desc
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        AS first_link_target

    FROM {{.input_schema}}.com_snowplowanalytics_snowplow_link_click_1 lc

    INNER JOIN {{.scratch_schema}}.events_staged{{.entropy}} ev
      ON lc.root_id = ev.event_id AND lc.root_tstamp = ev.collector_tstamp

    WHERE
      lc.root_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.base_run_limits{{.entropy}})
      AND lc.root_tstamp <= (SELECT upper_limit FROM {{.scratch_schema}}.base_run_limits{{.entropy}})
  ),

  --  using page_views_staged so needs to run after page views
  engagement AS (

    SELECT
      page_view_id,

      CASE
        WHEN engaged_time_in_s = 0 THEN TRUE
        ELSE FALSE
      END AS bounced_page_view,

      (vertical_percentage_scrolled / 100) * 0.3 + (engaged_time_in_s / 600) * 0.7 AS engagement_score

    FROM {{.scratch_schema}}.page_views_staged{{.entropy}}
  )

  SELECT
    ev.page_view_id,

    link_clicks,
    first_link_target,

    bounced_page_view,
    engagement_score,

    CASE
      WHEN ev.refr_medium = 'search'
       AND (lower(ev.mkt_medium) SIMILAR TO '%(cpc|ppc|sem|paidsearch)%'
         OR lower(ev.mkt_source) SIMILAR TO '%(cpc|ppc|sem|paidsearch)%') THEN 'paidsearch'
      WHEN lower(ev.mkt_medium) ILIKE '%paidsearch%'
       OR lower(ev.mkt_source) ILIKE '%paidsearch%' THEN 'paidsearch'
      WHEN lower(ev.mkt_source) SIMILAR TO '%(adwords|google_paid|googleads)%'
       OR lower(ev.mkt_medium) SIMILAR TO '%(adwords|google_paid|googleads)%' THEN 'paidsearch'
      WHEN ev.mkt_source ILIKE '%google%'
       AND ev.mkt_medium ILIKE '%ads%' THEN 'paidsearch'
      WHEN ev.refr_urlhost in ('www.googleadservices.com','googleads.g.doubleclick.net') then 'paidsearch'
      WHEN lower(ev.mkt_medium) SIMILAR TO '%(cpv|cpa|cpp|content-text|advertising|ads)%' THEN 'advertising'
      WHEN lower(ev.mkt_medium) SIMILAR TO '%(display|cpm|banner)%' THEN 'display'
      WHEN ev.refr_medium IS NULL   AND ev.page_url NOT ILIKE '%utm_%' THEN 'direct'
      WHEN (LOWER(ev.refr_medium) = 'search' AND ev.mkt_medium IS NULL)
       OR (LOWER(ev.refr_medium) = 'search' AND LOWER(ev.mkt_medium) = 'organic') THEN 'organicsearch'
      WHEN ev.refr_medium = 'social'
       OR REGEXP_COUNT(LOWER(ev.mkt_source),'^((.*(facebook|linkedin|instagram|insta|slideshare|social|tweet|twitter|youtube|lnkd|pinterest|googleplus|instagram|plus.google.com|quora|reddit|t.co|twitch|viadeo|xing|youtube).*)|(yt|fb|li))$')>0
       OR REGEXP_COUNT(LOWER(ev.mkt_medium),'^(.*)(social|facebook|linkedin|twitter|instagram|tweet)(.*)$')>0 THEN 'social'
      WHEN ev.refr_medium = 'email'
       OR ev.mkt_medium ILIKE '_mail' THEN 'email'
      WHEN ev.mkt_medium ILIKE 'affiliate' THEN 'affiliate'
      WHEN ev.refr_medium = 'unknown' or lower(ev.mkt_medium) ILIKE 'referral' OR lower(ev.mkt_medium) ILIKE 'referal' THEN 'referral'
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
