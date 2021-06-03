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

CREATE OR REPLACE PROCEDURE {{.scratch_schema}}.sessions_model (skip_derived BOOL,
                                                                stage_next BOOL
                                                                )
OPTIONS(strict_mode=false)
BEGIN
-- TODO: Evaluate if currently nested procedures should be moved to this level - eg. metadata committing.

  -- Setup sessions  
  CALL {{.scratch_schema}}.metadata_setup ();
  CALL {{.scratch_schema}}.init_metadata_logging('sessions');
  CALL {{.scratch_schema}}.setup_sessions();
  -- Run model
  CALL {{.scratch_schema}}.sessions_aggregates();
  CALL {{.scratch_schema}}.sessions_lasts();
  CALL {{.scratch_schema}}.sessions_this_run();
  CALL {{.scratch_schema}}.stage_step_metadata('sessions', 'domain_sessionid', 'start_tstamp', '{{.scratch_schema}}.sessions_this_run{{.entropy}}');
  CALL {{.scratch_schema}}.sessions_prep_manifest();
  CALL {{.scratch_schema}}.commit_sessions(skip_derived, stage_next);
END;

-- CALL {{.scratch_schema}}.sessions_model(FALSE, TRUE);
