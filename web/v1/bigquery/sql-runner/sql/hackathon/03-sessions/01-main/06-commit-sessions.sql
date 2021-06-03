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

CREATE OR REPLACE PROCEDURE {{.scratch_schema}}.commit_sessions (skip_derived BOOL, stage_next BOOL)
OPTIONS(strict_mode=false)
BEGIN

  IF NOT skip_derived THEN

    -- Commit to production if enabled
    -- Note: Automigrate hardcoded to false as all columns are to be explicitly defined in model.
    CALL {{.output_schema}}.commit_table('{{.scratch_schema}}',             -- sourceDataset
                                         'sessions_this_run{{.entropy}}',   -- sourceTable
                                         '{{.output_schema}}',              -- targetDataset
                                         'sessions{{.entropy}}',            -- targetTable
                                         'domain_sessionid',                -- joinKey
                                         'start_tstamp',                    -- partitionKey
                                         FALSE);                            -- automigrate

  END IF;

  IF stage_next THEN
  -- TODO: Figure out if this should be handled via call to commit table, or just manual.

    -- Commit staging manifest if enabled
    DELETE FROM {{.scratch_schema}}.sessions_userid_manifest_staged{{.entropy}}
      WHERE domain_userid IN (SELECT domain_userid FROM {{.scratch_schema}}.sessions_userid_manifest_this_run{{.entropy}});

    INSERT INTO {{.scratch_schema}}.sessions_userid_manifest_staged{{.entropy}}
      (SELECT * FROM {{.scratch_schema}}.sessions_userid_manifest_this_run{{.entropy}});
  
  END IF;

  -- TODO: Figure out where calls to the cleanup logging need to go for all these tables.

  CALL {{.scratch_schema}}.commit_step_metadata('sessions');

END;
