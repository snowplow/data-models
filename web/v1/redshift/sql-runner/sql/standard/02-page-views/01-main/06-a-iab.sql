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

DROP TABLE IF EXISTS {{.scratch_schema}}.pv_addon_iab{{.entropy}};

CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.pv_addon_iab{{.entropy}} (
  page_view_id CHAR(36),
  category VARCHAR,
  primary_impact VARCHAR,
  reason VARCHAR,
  spider_or_robot BOOLEAN
)
DISTSTYLE KEY
DISTKEY (page_view_id)
SORTKEY (page_view_id);

{{if eq .iab true}}
  INSERT INTO {{.scratch_schema}}.pv_addon_iab{{.entropy}} (
    SELECT

      pv.page_view_id,

      iab.category,
      iab.primary_impact,
      iab.reason,
      iab.spider_or_robot

    FROM {{.input_schema}}.com_iab_snowplow_spiders_and_robots_1 iab

    INNER JOIN {{.scratch_schema}}.pv_page_view_events{{.entropy}} pv
      ON iab.root_id = pv.event_id
      AND iab.root_tstamp = pv.collector_tstamp

    WHERE iab.root_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.pv_run_limits{{.entropy}})
      AND iab.root_tstamp <= (SELECT upper_limit FROM {{.scratch_schema}}.pv_run_limits{{.entropy}})
  );
{{end}}
