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

BEGIN;

  DROP TABLE IF EXISTS {{.scratch_schema}}.pv_page_view_id_duplicates_this_run{{.entropy}};

  CREATE TABLE {{.scratch_schema}}.pv_page_view_id_duplicates_this_run{{.entropy}}
    DISTSTYLE KEY
    DISTKEY (page_view_id)
    SORTKEY (page_view_id)
  AS(
    SELECT
      page_view_id,
      count(*) AS num_rows,
      count(DISTINCT event_id) AS dist_event_ids

    FROM
      {{.scratch_schema}}.pv_page_view_events{{.entropy}}

    GROUP BY 1
    HAVING count(*) > 1
  );

  -- Remove duplicates from the table
  DELETE FROM {{.scratch_schema}}.pv_page_view_events{{.entropy}} WHERE page_view_id IN (SELECT page_view_id FROM {{.scratch_schema}}.pv_page_view_id_duplicates_this_run{{.entropy}});

END;
