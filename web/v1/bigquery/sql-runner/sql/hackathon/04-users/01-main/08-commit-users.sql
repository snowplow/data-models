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

CREATE OR REPLACE PROCEDURE {{.scratch_schema}}.commit_users (skip_derived BOOL)
OPTIONS(strict_mode=false)
BEGIN

  IF NOT skip_derived THEN

    -- Commit to production if enabled
    -- Note: Automigrate hardcoded to false as all columns are to be explicitly defined in model.
    CALL {{.output_schema}}.commit_table('{{.scratch_schema}}',             -- sourceDataset
                                         'users_this_run{{.entropy}}',   -- sourceTable
                                         '{{.output_schema}}',              -- targetDataset
                                         'users{{.entropy}}',            -- targetTable
                                         'domain_userid',                -- joinKey
                                         'start_tstamp',                    -- partitionKey
                                         FALSE);                            -- automigrate

  END IF;
  
  CALL {{.scratch_schema}}.commit_step_metadata('users');

END;
