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

CREATE OR REPLACE TABLE {{.scratch_schema}}.mobile_users_lasts{{.entropy}}
AS(
  
  SELECT
    a.device_user_id,
    a.last_screen_view_name,
    a.last_screen_view_transition_type,
    a.last_screen_view_type,

    a.carrier AS last_carrier,
    a.os_version AS last_os_version

  FROM
    {{.scratch_schema}}.mobile_users_sessions_this_run{{.entropy}} a

  INNER JOIN 
    {{.scratch_schema}}.mobile_users_aggregates{{.entropy}} b
    ON a.device_user_id = b.device_user_id
    AND a.end_tstamp = b.end_tstamp

);
