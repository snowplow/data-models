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

-- Destructive operation which destroys every table involved in the model, including prod. 
-- Only to be used for a full recompute
CREATE OR REPLACE PROCEDURE {{.scratch_schema}}.XX_destroy_model_tables ()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE TABLE_PATHS_TO_DROP ARRAY<STRING>;
  DECLARE i INT64 DEFAULT 0;
  
  -- Get all tables logged
  SET TABLE_PATHS_TO_DROP = ARRAY(SELECT table_path FROM {{.scratch_schema}}.model_tables{{.entropy}});

  -- Loop through and drop them all
  LOOP 
    SET i = i + 1;
    IF i > ARRAY_LENGTH(TABLE_PATHS_TO_DROP) THEN 
      LEAVE;
    END IF;
    
    EXECUTE IMMEDIATE CONCAT("DROP TABLE IF EXISTS ", TABLE_PATHS_TO_DROP[ORDINAL(i)], ";");
    
  END LOOP; 
  
  -- Drop the model_tables table
  DROP TABLE IF EXISTS {{.scratch_schema}}.model_tables{{.entropy}};
END;

-- Destructive operation which destroys every procedure involved in the model, including prod.
-- Only to be used for a full decommission
CREATE OR REPLACE PROCEDURE {{.scratch_schema}}.XX_destroy_model_procedures ()
OPTIONS(strict_mode=false)
BEGIN
  -- commit_procedures
  DROP FUNCTION IF EXISTS {{.output_schema}}.columnCheckQuery;
  DROP PROCEDURE IF EXISTS {{.output_schema}}.commit_table;
  
  -- metadata_procedures
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.metadata_setup;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.init_metadata_logging;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.stage_step_metadata;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.commit_step_metadata;
  
  -- utility_procedures
  DROP PROCEDURE IF EXISTS {{.output_schema}}.combine_context_versions;
  
  -- cleanup_procedures
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.log_model_table;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.cleanup_model_tables;
  
  -- Destructive procedures
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.XX_destroy_model_tables;
  
  -- Model procedures
  -- Base
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.setup_base;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.new_events_limits;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.run_manifest;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.sessions_to_process;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.sessions_to_include;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.batch_limits;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.events_this_run;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.commit_base;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.base_model;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.commit_base_manifest;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.base_complete;
  -- Page Views
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.setup_page_views;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.page_view_events;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.page_view_engaged_time;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.pv_scroll_depth;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.standard_optional_contexts;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.page_views_this_run;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.commit_page_views;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.page_views_model;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.page_views_complete;
  -- Sessions
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.setup_sessions;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.sessions_aggregates;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.sessions_lasts;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.sessions_this_run;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.sessions_prep_manifest;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.commit_sessions;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.sessions_model;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.sessions_complete;
  -- Users
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.setup_users;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.userids_this_run;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.users_limits;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.users_sessions_this_run;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.users_aggregates;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.users_lasts;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.users_this_run;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.commit_users;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.users_model;
  DROP PROCEDURE IF EXISTS {{.scratch_schema}}.users_complete;
  
  -- Finally, this procedure drops itself. Magic.
  DROP PROCEDURE {{.scratch_schema}}.XX_destroy_model_procedures;
END
