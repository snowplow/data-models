
CREATE OR REPLACE TABLE {{.scratch_schema}}.pv_scroll_depth{{.entropy}}
AS (
  WITH prep AS (

    SELECT
      ev.page_view_id,

      MAX(ev.doc_width) AS doc_width,
      MAX(ev.doc_height) AS doc_height,

      MAX(ev.br_viewwidth) AS br_viewwidth,
      MAX(ev.br_viewheight) AS br_viewheight,

      -- NVL replaces NULL with 0 (because the page view event does send an offset)
      -- GREATEST prevents outliers (negative offsets)
      -- LEAST also prevents outliers (offsets greater than the docwidth or docheight)

      LEAST(GREATEST(MIN(COALESCE(pp_xoffset_min, 0)), 0), MAX(doc_width)) AS hmin, -- should be zero
      LEAST(GREATEST(MAX(COALESCE(pp_xoffset_max, 0)), 0), MAX(doc_width)) AS hmax,

      LEAST(GREATEST(MIN(COALESCE(pp_yoffset_min, 0)), 0), MAX(doc_height)) AS vmin, -- should be zero (edge case: not zero because the pv event is missing - but these are not in scratch.dev_pv_01 so not an issue)
      LEAST(GREATEST(MAX(COALESCE(pp_yoffset_max, 0)), 0), MAX(doc_height)) AS vmax

    FROM
      {{.scratch_schema}}.events_staged{{.entropy}} AS ev

    WHERE ev.event_name IN ('page_view', 'page_ping')
      AND ev.doc_height > 0 -- exclude problematic (but rare) edge case
      AND ev.doc_width > 0 -- exclude problematic (but rare) edge case

    GROUP BY 1
  )

  SELECT

    page_view_id,

    doc_width,
    doc_height,

    br_viewwidth,
    br_viewheight,

    hmin,
    hmax,
    vmin,
    vmax,

        ROUND(100*(GREATEST(hmin, 0)/CAST(doc_width AS FLOAT64))) AS relative_hmin, -- brackets matter: because hmin is of type INT, we need to divide before we multiply by 100 or we risk an overflow
        ROUND(100*(LEAST(hmax + br_viewwidth, doc_width)/CAST(doc_width AS FLOAT64))) AS relative_hmax,
        ROUND(100*(GREATEST(vmin, 0)/CAST(doc_height AS FLOAT64))) AS relative_vmin,
        ROUND(100*(LEAST(vmax + br_viewheight, doc_height)/CAST(doc_height AS FLOAT64))) AS relative_vmax -- not zero when a user hasn't scrolled because it includes the non-zero viewheight

  FROM prep
);

