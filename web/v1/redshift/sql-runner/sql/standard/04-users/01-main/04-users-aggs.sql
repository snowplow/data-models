/*
   Copyright 2020-2021 Snowplow Analytics Ltd. All rights reserved.

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

DROP TABLE IF EXISTS {{.scratch_schema}}.users_aggregates{{.entropy}};

CREATE TABLE {{.scratch_schema}}.users_aggregates{{.entropy}}
  DISTSTYLE KEY
  DISTKEY (domain_userid)
  SORTKEY (domain_userid)
AS(
  SELECT
    domain_userid,

    -- time
    MIN(start_tstamp) AS start_tstamp,
    MAX(end_tstamp) AS end_tstamp,

    -- engagement
    SUM(page_views) AS page_views,
    COUNT(DISTINCT domain_sessionid) AS sessions,
    SUM(engaged_time_in_s) AS engaged_time_in_s

  FROM {{.scratch_schema}}.users_sessions_this_run{{.entropy}}

  GROUP BY 1
);
