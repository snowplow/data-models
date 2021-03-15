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

-- Create a limit for this run - single value table.
DROP TABLE IF EXISTS {{.scratch_schema}}.mobile_users_userids_this_run{{.entropy}};

CREATE TABLE {{.scratch_schema}}.mobile_users_userids_this_run{{.entropy}}
  DISTSTYLE KEY
  DISTKEY (device_user_id)
  SORTKEY (device_user_id)
AS(
  SELECT
    a.device_user_id,
    LEAST(a.start_tstamp, b.start_tstamp) AS start_tstamp

  FROM
    {{.scratch_schema}}.mobile_sessions_userid_manifest_staged{{.entropy}} a
  LEFT JOIN
    {{.output_schema}}.mobile_users_manifest{{.entropy}} b
    ON a.device_user_id = b.device_user_id
);
