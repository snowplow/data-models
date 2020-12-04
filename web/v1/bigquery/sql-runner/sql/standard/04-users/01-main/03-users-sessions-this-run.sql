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

DECLARE LOWER_LIMIT, UPPER_LIMIT TIMESTAMP;

SET (LOWER_LIMIT, UPPER_LIMIT) = (SELECT AS STRUCT lower_limit, upper_limit FROM {{.scratch_schema}}.users_limits{{.entropy}});

-- Create a limit for this run - single value table.
CREATE OR REPLACE TABLE {{.scratch_schema}}.users_sessions_this_run{{.entropy}}
AS(
  SELECT
    a.*
  FROM {{.output_schema}}.sessions{{.entropy}} a
  INNER JOIN {{.scratch_schema}}.users_userids_this_run{{.entropy}} b
  ON a.domain_userid = b.domain_userid

  WHERE a.start_tstamp >= LOWER_LIMIT
  AND   a.start_tstamp <= UPPER_LIMIT
);
