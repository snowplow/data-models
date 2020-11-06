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

DROP TABLE IF EXISTS {{.scratch_schema}}.pv_scroll_depth{{.entropy}};

CREATE TABLE {{.scratch_schema}}.pv_scroll_depth{{.entropy}}
  DISTSTYLE KEY
  DISTKEY (page_view_id)
  SORTKEY (page_view_id)
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

      LEAST(GREATEST(MIN(NVL(ev.pp_xoffset_min, 0)), 0), MAX(ev.doc_width)) AS hmin, -- should be zero
      LEAST(GREATEST(MAX(NVL(ev.pp_xoffset_max, 0)), 0), MAX(ev.doc_width)) AS hmax,

      LEAST(GREATEST(MIN(NVL(ev.pp_yoffset_min, 0)), 0), MAX(ev.doc_height)) AS vmin, -- should be zero (edge case: not zero because the pv event is missing)
      LEAST(GREATEST(MAX(NVL(ev.pp_yoffset_max, 0)), 0), MAX(ev.doc_height)) AS vmax

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

    ROUND(100*(GREATEST(hmin, 0)/doc_width::FLOAT))::DOUBLE PRECISION AS relative_hmin, -- brackets matter: because hmin is of type INT, we need to divide before we multiply by 100 or we risk an overflow
    ROUND(100*(LEAST(hmax + br_viewwidth, doc_width)/doc_width::FLOAT))::DOUBLE PRECISION AS relative_hmax,
    ROUND(100*(GREATEST(vmin, 0)/doc_height::FLOAT))::DOUBLE PRECISION AS relative_vmin,
    ROUND(100*(LEAST(vmax + br_viewheight, doc_height)/doc_height::FLOAT))::DOUBLE PRECISION AS relative_vmax -- not zero when a user hasn't scrolled because it includes the non-zero viewheight

  FROM prep
);
