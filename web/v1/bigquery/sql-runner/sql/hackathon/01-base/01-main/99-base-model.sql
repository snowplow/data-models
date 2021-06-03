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

CREATE OR REPLACE PROCEDURE {{.scratch_schema}}.base_model (start_date STRING, 
                                                            lookback_window_hours INT64,
                                                            update_cadence_days INT64,
                                                            session_lookback_days INT64,
                                                            days_late_allowed INT64,
                                                            skip_derived BOOL, 
                                                            stage_next BOOL
                                                          )
OPTIONS(strict_mode=false)
BEGIN
  -- Setup model
  CALL {{.scratch_schema}}.metadata_setup ();
  CALL {{.scratch_schema}}.init_metadata_logging('base');
  CALL {{.scratch_schema}}.setup_base(start_date);
  -- Run base module
  CALL {{.scratch_schema}}.new_events_limits(lookback_window_hours, update_cadence_days, session_lookback_days );
  CALL {{.scratch_schema}}.run_manifest();
  CALL {{.scratch_schema}}.sessions_to_process(days_late_allowed);
  CALL {{.scratch_schema}}.sessions_to_include(days_late_allowed);
  CALL {{.scratch_schema}}.batch_limits();
  CALL {{.scratch_schema}}.events_this_run();
  CALL {{.scratch_schema}}.stage_step_metadata('base', 'event_id', 'collector_tstamp', '{{.scratch_schema}}.events_this_run{{.entropy}}');
  CALL {{.scratch_schema}}.commit_base (skip_derived, stage_next);

END;
