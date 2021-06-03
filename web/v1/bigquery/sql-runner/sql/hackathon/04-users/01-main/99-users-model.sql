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

CREATE OR REPLACE PROCEDURE {{.scratch_schema}}.users_model (skip_derived BOOL)
BEGIN

  -- Setup users
  CALL {{.scratch_schema}}.metadata_setup ();
  CALL {{.scratch_schema}}.init_metadata_logging('users');
  CALL {{.scratch_schema}}.setup_users('{{.start_date}}');
  
  -- Run users
  CALL {{.scratch_schema}}.userids_this_run();
  CALL {{.scratch_schema}}.users_limits();
  CALL {{.scratch_schema}}.users_sessions_this_run();
  CALL {{.scratch_schema}}.users_aggregates();
  CALL {{.scratch_schema}}.users_lasts();
  CALL {{.scratch_schema}}.users_this_run();
  CALL {{.scratch_schema}}.stage_step_metadata('users', 'domain_userid', 'start_tstamp', '{{.scratch_schema}}.users_this_run{{.entropy}}');
  CALL {{.scratch_schema}}.commit_users(skip_derived);

END;

-- CALL {{.scratch_schema}}.users_model({{.skip_derived}});
