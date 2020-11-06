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

DROP TABLE IF EXISTS {{.scratch_schema}}.pv_engaged_time{{.entropy}};

CREATE TABLE {{.scratch_schema}}.pv_engaged_time{{.entropy}}
	DISTSTYLE KEY
	DISTKEY (page_view_id)
	SORTKEY (page_view_id)
AS(
	SELECT

    ev.page_view_id,
    MAX(ev.derived_tstamp) AS end_tstamp,

    -- Aggregate pings:
      -- Divides EPOCH tstamps by heartbeat to get distinct intervals
      -- FLOOR rounds to nearest integer - duplicates all evaluate to the same number
      -- count(DISTINCT) counts duplicates only once
      -- adding minimumVisitLength accounts for the page view event itself.

    {{.heartbeat}} * (COUNT(DISTINCT(FLOOR(EXTRACT(EPOCH FROM ev.derived_tstamp)/{{.heartbeat}}))) - 1) + {{.minimumVisitLength}} AS engaged_time_in_s

	FROM {{.scratch_schema}}.events_staged{{.entropy}} AS ev

	WHERE ev.event_name = 'page_ping'
    AND ev.collector_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.pv_run_limits{{.entropy}})
    AND ev.collector_tstamp <= (SELECT upper_limit FROM {{.scratch_schema}}.pv_run_limits{{.entropy}})

  GROUP BY 1

);
