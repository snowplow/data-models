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


CREATE OR REPLACE PROCEDURE {{.scratch_schema}}.users_complete (level STRING)
OPTIONS(strict_mode=false)
BEGIN

  -- Update manifest
  DELETE
    FROM {{.output_schema}}.users_manifest{{.entropy}}
    WHERE domain_userid IN (SELECT domain_userid FROM {{.scratch_schema}}.users_userids_this_run{{.entropy}});

  INSERT INTO {{.output_schema}}.users_manifest{{.entropy}} (SELECT * FROM {{.scratch_schema}}.users_userids_this_run{{.entropy}});

  -- Truncate input table 
  TRUNCATE TABLE {{.scratch_schema}}.sessions_userid_manifest_staged{{.entropy}};
  
  CALL {{.scratch_schema}}.cleanup_model_tables(level, 'sessions', TRUE);

END;

-- CALL {{.scratch_schema}}.users_complete ('debug');
