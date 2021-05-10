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

-- 1. Aggregate with a drop and recompute logic

CREATE OR REPLACE TABLE {{.scratch_schema}}.session_goals_staged{{.entropy}} AS (

  WITH goals AS (

    SELECT
      sv.session_id,
      BOOLOR_AGG(sv.screen_view_name = 'registration') AS has_started_registration,
      BOOLOR_AGG(sv.screen_view_name = 'my_account') AS has_completed_registration,
      BOOLOR_AGG(sv.screen_view_name = 'search_results') AS has_used_search,
      BOOLOR_AGG(sv.screen_view_name = 'products') AS has_viewed_products

    FROM
      {{.scratch_schema}}.mobile_screen_views_staged{{.entropy}} sv

    GROUP BY 1

  )

  SELECT
    s.session_id,
    s.start_tstamp,
    g.has_started_registration,
    g.has_completed_registration,
    g.has_used_search,
    g.has_viewed_products,
    IFF(g.has_started_registration AND g.has_completed_registration AND g.has_used_search AND g.has_viewed_products, TRUE, FALSE) AS has_completed_goals

  FROM
    {{.scratch_schema}}.mobile_sessions_this_run{{.entropy}} AS s --select from mobile_sessions_this_run to get start_tstamp. Screen view might not be start of session
  INNER JOIN goals AS g
    ON s.session_id = g.session_id

);
