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

CREATE OR REPLACE PROCEDURE {{.scratch_schema}}.setup_base (start_date STRING)
OPTIONS(strict_mode=false)
BEGIN
  -- Setup manifests
  CREATE TABLE IF NOT EXISTS {{.output_schema}}.base_event_id_manifest{{.entropy}}
  PARTITION BY DATE(collector_tstamp)
  AS (
    SELECT
      'seed' AS event_id,
      TIMESTAMP(start_date) AS collector_tstamp
      -- TODO: Design decision: convert to timestamp here or pass timestamp arg?
  );

  CREATE TABLE IF NOT EXISTS {{.output_schema}}.base_session_id_manifest{{.entropy}}
  PARTITION BY DATE(min_tstamp)
  AS (
    SELECT
      'seed' AS session_id,
      TIMESTAMP(start_date) AS min_tstamp
  );
  
  CALL {{.scratch_schema}}.log_model_table('{{.output_schema}}.base_event_id_manifest{{.entropy}}', 'prod', 'base');
  CALL {{.scratch_schema}}.log_model_table('{{.output_schema}}.base_session_id_manifest{{.entropy}}', 'prod', 'base');
END;
