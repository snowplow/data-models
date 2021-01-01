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


SET ALL_ROWS_PV = (SELECT COUNT(*) FROM {{.scratch_schema}}.pv_page_view_events{{.entropy}});

-- Keep only first of duplicates
INSERT OVERWRITE INTO {{.scratch_schema}}.pv_page_view_events{{.entropy}}
  WITH cte_tmp AS (
    SELECT
      *

    FROM
      {{.scratch_schema}}.pv_page_view_events{{.entropy}}

    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY page_view_id ORDER BY derived_tstamp) = 1
  )
  SELECT * FROM cte_tmp;

CREATE OR REPLACE TABLE {{.scratch_schema}}.pv_run_dupe_metadata_temp{{.entropy}}
AS (
  SELECT
    'run' AS id,
    $ALL_ROWS_PV - COUNT(*) AS duplicate_rows_removed

  FROM
    {{.scratch_schema}}.pv_page_view_events{{.entropy}}
);

UNSET ALL_ROWS_PV;
