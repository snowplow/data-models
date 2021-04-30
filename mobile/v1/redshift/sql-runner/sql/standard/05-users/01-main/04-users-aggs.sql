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

DROP TABLE IF EXISTS {{.scratch_schema}}.mobile_users_aggregates{{.entropy}};

CREATE TABLE {{.scratch_schema}}.mobile_users_aggregates{{.entropy}}
  DISTSTYLE KEY
  DISTKEY (device_user_id)
  SORTKEY (device_user_id)
AS(
  SELECT
    device_user_id,
    -- time
    MIN(start_tstamp) AS start_tstamp,
    MAX(end_tstamp) AS end_tstamp,
    -- engagement
    SUM(screen_views) AS screen_views,
    SUM(screen_names_viewed) AS screen_names_viewed,
    COUNT(DISTINCT session_id) AS sessions,
    SUM(session_duration_s) AS sessions_duration_s,
    COUNT(DISTINCT DATE_TRUNC('d', start_tstamp)) AS active_days,
    --errors
    SUM(app_errors) AS app_errors,
    SUM(fatal_app_errors) AS fatal_app_errors

  FROM {{.scratch_schema}}.mobile_users_sessions_this_run{{.entropy}}

  GROUP BY 1
);
