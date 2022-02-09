/*
   Copyright 2021-2022 Snowplow Analytics Ltd. All rights reserved.

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


CREATE OR REPLACE TABLE {{.scratch_schema}}.users_userids_this_run{{.entropy}}
AS (
  SELECT
    a.domain_userid,
    -- LEAST produces NULL if any input value is null
    LEAST(a.start_tstamp, COALESCE(b.start_tstamp, a.start_tstamp)) AS start_tstamp

  FROM
    {{.scratch_schema}}.sessions_userid_manifest_staged{{.entropy}} AS a

  LEFT JOIN {{.output_schema}}.users_manifest{{.entropy}} AS b
    ON a.domain_userid = b.domain_userid
);
