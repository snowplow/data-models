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


-- TODO: Roll this up into something more generic?

CREATE OR REPLACE PROCEDURE {{.scratch_schema}}.commit_page_views (skip_derived BOOL, stage_next BOOL)
OPTIONS(strict_mode=false)
BEGIN
  
  IF NOT skip_derived THEN

    -- Commit to production if enabled
    -- Note: Automigrate hardcoded to false as all columns are to be explicitly defined in model.
    CALL {{.output_schema}}.commit_table('{{.scratch_schema}}',             -- sourceDataset
                                         'page_views_this_run{{.entropy}}', -- sourceTable
                                         '{{.output_schema}}',              -- targetDataset
                                         'page_views{{.entropy}}',          -- targetTable
                                         'page_view_id',                    -- joinKey
                                         'start_tstamp',                    -- partitionKey
                                         FALSE);                            -- automigrate

  END IF;

  IF stage_next THEN

    -- Commit staging table if enabled
    -- Note: Automigrate hardcoded to false as all columns are to be explicitly defined in model.
    CALL {{.output_schema}}.commit_table('{{.scratch_schema}}',             -- sourceDataset
                                         'page_views_this_run{{.entropy}}', -- sourceTable
                                         '{{.scratch_schema}}',              -- targetDataset
                                         'page_views_staged{{.entropy}}',          -- targetTable
                                         'page_view_id',                    -- joinKey
                                         'start_tstamp',                    -- partitionKey
                                         FALSE);                            -- automigrate

  END IF;

  CALL {{.scratch_schema}}.commit_step_metadata('page_views');
END;
