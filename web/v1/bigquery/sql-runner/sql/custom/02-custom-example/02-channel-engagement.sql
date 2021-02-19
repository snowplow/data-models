CREATE OR REPLACE TABLE {{.scratch_schema}}.channel_engagement_staged{{.entropy}} AS(
  SELECT
    -- get some basic info
    pv.page_view_id,
    pv.start_tstamp,
    pv.page_url,

    -- Add in our custom link click metrics
    lc.link_clicks,
    lc.first_link_clicked,
    lc.last_link_clicked,

    -- channel definition
    CASE
      WHEN pv.refr_medium = 'search'
       AND (REGEXP_CONTAINS(lower(pv.mkt_medium), '%(cpc|ppc|sem|paidsearch)%')
         OR REGEXP_CONTAINS(lower(pv.mkt_source), '%(cpc|ppc|sem|paidsearch)%')) THEN 'paidsearch'
      WHEN lower(pv.mkt_medium) LIKE '%paidsearch%'
       OR lower(pv.mkt_source) LIKE '%paidsearch%' THEN 'paidsearch'
      WHEN REGEXP_CONTAINS(lower(pv.mkt_source), '%(adwords|google_paid|googleads)%')
       OR REGEXP_CONTAINS(lower(pv.mkt_medium), '%(adwords|google_paid|googleads)%') THEN 'paidsearch'
      WHEN lower(pv.mkt_source) LIKE '%google%'

       AND lower(pv.mkt_medium) LIKE '%ads%' THEN 'paidsearch'
      WHEN pv.refr_urlhost in ('www.googleadservices.com','googleads.g.doubleclick.net') then 'paidsearch'

      WHEN REGEXP_CONTAINS(lower(pv.mkt_medium), '%(cpv|cpa|cpp|content-text|advertising|ads)%') THEN 'advertising'
      WHEN REGEXP_CONTAINS(lower(pv.mkt_medium), '%(display|cpm|banner)%') THEN 'display'

      WHEN pv.refr_medium IS NULL AND pv.page_url NOT LIKE '%utm_%' THEN 'direct'
      WHEN (LOWER(pv.refr_medium) = 'search' AND pv.mkt_medium IS NULL)
       OR (LOWER(pv.refr_medium) = 'search' AND LOWER(pv.mkt_medium) = 'organic') THEN 'organicsearch'
      WHEN pv.refr_medium = 'social'
       OR REGEXP_CONTAINS(LOWER(pv.mkt_source),'^((.*(facebook|linkedin|instagram|insta|slideshare|social|tweet|twitter|youtube|lnkd|pinterest|googleplus|instagram|plus.google.com|quora|reddit|t.co|twitch|viadeo|xing|youtube).*)|(yt|fb|li))$')
       OR REGEXP_CONTAINS(LOWER(pv.mkt_medium),'^(.*)(social|facebook|linkedin|twitter|instagram|tweet)(.*)$') THEN 'social'
      WHEN pv.refr_medium = 'email'
       OR lower(pv.mkt_medium) LIKE '_mail' THEN 'email'
      WHEN lower(pv.mkt_medium) LIKE 'affiliate' THEN 'affiliate'
      WHEN pv.refr_medium = 'unknown' or lower(pv.mkt_medium) LIKE 'referral' OR lower(pv.mkt_medium) LIKE 'referal' THEN 'referral'
      WHEN pv.refr_medium = 'internal' then 'internal'
      ELSE 'others'
    END AS channel,

    -- some metrics to measure engagement
    CASE
      WHEN pv.engaged_time_in_s = 0 THEN TRUE
      ELSE FALSE
    END AS bounced_page_view,

    (pv.vertical_percentage_scrolled / 100) * 0.3 + (pv.engaged_time_in_s / 600) * 0.7 AS engagement_score

    FROM
    -- Query from staged table to get most recent according to incremental logic
      {{.scratch_schema}}.page_views_staged{{.entropy}} pv
    LEFT JOIN
    -- Join on the joinkey
      {{.scratch_schema}}.link_clicks{{.entropy}} lc
    ON pv.page_view_id = lc.page_view_id
)
