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

-- TODO: Wrap this into the previous?
CREATE OR REPLACE PROCEDURE {{.scratch_schema}}.users_limits ()
OPTIONS(strict_mode=false)
BEGIN
  -- Create a limit for this run
  CREATE OR REPLACE TABLE {{.scratch_schema}}.users_limits{{.entropy}} AS(
    SELECT
      MIN(start_tstamp) AS lower_limit,
      MAX(start_tstamp) AS upper_limit

    FROM
      {{.scratch_schema}}.users_userids_this_run{{.entropy}}
  );
  
  CALL {{.scratch_schema}}.log_model_table('{{.scratch_schema}}.users_limits{{.entropy}}', 'debug', 'users');
END;
