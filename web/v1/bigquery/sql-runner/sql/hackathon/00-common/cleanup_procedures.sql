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

-- TODO: Make this more efficient - it throws errors due to too many table updates
-- One approach is to have each function return an array of value sets, then at the end of the main model function perform this operation all at once.
-- Drawback is that failing mid-way through might make us miss some - could try to avoid by error handling it also.
CREATE OR REPLACE PROCEDURE {{.scratch_schema}}.log_model_table (tpath STRING, tlevel STRING, tmodule STRING)
OPTIONS(strict_mode=false)
BEGIN
  CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.model_tables{{.entropy}} (table_path STRING, level STRING, module STRING);
  
  IF NOT EXISTS (SELECT tpath FROM {{.scratch_schema}}.model_tables{{.entropy}} WHERE table_path = tpath) THEN
    INSERT INTO {{.scratch_schema}}.model_tables{{.entropy}} VALUES(tpath, tlevel, tmodule);
  END IF;
END;

CREATE OR REPLACE PROCEDURE {{.scratch_schema}}.cleanup_model_tables (tlevel STRING, step_name STRING, ends_run BOOL)
OPTIONS(strict_mode=false)
BEGIN
  DECLARE LEVEL_FILTER STRING;
  DECLARE STEP_FILTER STRING;
  DECLARE TABLE_PATHS_TO_DROP ARRAY<STRING>;
  DECLARE i INT64 DEFAULT 0;
  
  IF tlevel = 'trace' THEN
    RETURN;
  
  ELSEIF tlevel = 'debug' THEN
    SET LEVEL_FILTER = "('trace')";
    
  ELSEIF tlevel = 'all' THEN
    SET LEVEL_FILTER = "('trace', 'debug')";
  END IF;
  
  IF step_name = 'all' THEN
    SET STEP_FILTER = "";
  ELSE 
    SET STEP_FILTER = CONCAT("AND module IN('", step_name, "')");
  END IF;
  
  EXECUTE IMMEDIATE CONCAT("SELECT ARRAY(SELECT table_path FROM {{.scratch_schema}}.model_tables{{.entropy}} WHERE level IN", LEVEL_FILTER, STEP_FILTER, ");") INTO TABLE_PATHS_TO_DROP;
  
  LOOP 
    SET i = i + 1;
    IF i > ARRAY_LENGTH(TABLE_PATHS_TO_DROP) THEN 
      LEAVE;
    END IF;
    
    EXECUTE IMMEDIATE CONCAT("DROP TABLE ", TABLE_PATHS_TO_DROP[ORDINAL(i)], ";");
    DELETE FROM {{.scratch_schema}}.model_tables{{.entropy}} WHERE table_path = TABLE_PATHS_TO_DROP[ORDINAL(i)];
  END LOOP; 
  
  IF ends_run THEN
    DROP TABLE IF EXISTS {{.scratch_schema}}.metadata_run_id{{.entropy}};
  END IF;
END;
