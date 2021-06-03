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

-- TODO: Rename? run_page_views() perhaps?

-- TODO: Attempt to simplify by removing templates in table names, if possible.
CREATE OR REPLACE PROCEDURE {{.scratch_schema}}.page_views_model (heartbeat INT64, 
                                                                  minimumVisitLength INT64, 
                                                                  iab BOOL,
                                                                  ua_parser BOOL,
                                                                  yauaa BOOL, 
                                                                  skip_derived BOOL,
                                                                  stage_next BOOL
                                                                )
OPTIONS(strict_mode=false)
BEGIN
-- TODO: Evaluate if currently nested procedures should be moved to this level - eg. metadata committing.

  -- Setup model 
  CALL {{.scratch_schema}}.metadata_setup ();
  CALL {{.scratch_schema}}.init_metadata_logging('page_views');
  CALL {{.scratch_schema}}.setup_page_views ();
  -- Run model steps
  CALL {{.scratch_schema}}.page_view_events ();
  CALL {{.scratch_schema}}.page_view_engaged_time (heartbeat, minimumVisitLength);
  CALL {{.scratch_schema}}.pv_scroll_depth ();
  CALL {{.scratch_schema}}.standard_optional_contexts (iab, ua_parser, yauaa);
  CALL {{.scratch_schema}}.page_views_this_run();
  CALL {{.scratch_schema}}.stage_step_metadata('page_views', 'page_view_id', 'start_tstamp', '{{.scratch_schema}}.page_views_this_run{{.entropy}}');
  -- Commit
  CALL {{.scratch_schema}}.commit_page_views (skip_derived, stage_next);
  
END;


-- TODO: Move these
-- CALL {{.scratch_schema}}.page_views_model ({{.heartbeat}}, {{.minimumVisitLength}}, {{.iab}}, {{.ua_parser}}, {{.yauaa}}, {{or .skip_derived false}}, {{.stage_next}});

-- CALL {{.scratch_schema}}.page_views_complete("{{.cleanup_mode}}", {{.skip_derived}});
