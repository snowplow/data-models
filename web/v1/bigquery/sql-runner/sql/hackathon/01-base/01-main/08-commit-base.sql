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

CREATE OR REPLACE PROCEDURE {{.scratch_schema}}.commit_base (skip_derived BOOL, stage_next BOOL)
OPTIONS(strict_mode=false)
BEGIN

  IF NOT skip_derived THEN
    -- Commit staging if enabled
    -- Note: automigrate is hardcoded to true here on purpose
    CALL {{.output_schema}}.commit_table('{{.scratch_schema}}',         -- sourceDataset
                                         'events_this_run{{.entropy}}', -- sourceTable
                                         '{{.scratch_schema}}',         -- targetDataset
                                         'events_staged{{.entropy}}',   -- targetTable
                                         'event_id',                    -- joinKey
                                         'collector_tstamp',            -- partitionKey
                                         TRUE);                         -- automigrate
  END IF;

  -- Commit metadata
  CALL {{.scratch_schema}}.commit_step_metadata('base');
  
  CALL {{.scratch_schema}}.log_model_table('{{.scratch_schema}}.events_staged{{.entropy}}', 'prod', 'base');
END;
