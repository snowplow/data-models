/*
   Copyright 2021 Snowplow Analytics Ltd. All rights reserved.

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

DROP TABLE IF EXISTS {{.scratch_schema}}.mobile_sessions_screen_view_details{{.entropy}};

CREATE TABLE {{.scratch_schema}}.mobile_sessions_screen_view_details{{.entropy}}
  DISTSTYLE KEY
  DISTKEY (session_id)
  SORTKEY (session_id)
AS (

	SELECT
		sv.session_id,
		COUNT(DISTINCT sv.screen_view_id) AS screen_views,
		COUNT(DISTINCT sv.screen_view_name) AS unique_screen_views,
		--Could split below into first/last scratch tables. Trying to minimise joins to sessions.
		MAX(CASE WHEN sv.screen_view_in_session_index = 1 THEN sv.screen_view_name END) AS first_screen_view_name,
		MAX(CASE WHEN sv.screen_view_in_session_index = 1 THEN sv.screen_view_transition_type END) AS first_screen_view_transition_type,
		MAX(CASE WHEN sv.screen_view_in_session_index = 1 THEN sv.screen_view_type END) AS first_screen_view_type,
		MAX(CASE WHEN sv.screen_view_in_session_index = sv.screen_views_in_session THEN sv.screen_view_name END) AS last_screen_view_name,
		MAX(CASE WHEN sv.screen_view_in_session_index = sv.screen_views_in_session THEN sv.screen_view_transition_type END) AS last_screen_view_transition_type,
		MAX(CASE WHEN sv.screen_view_in_session_index = sv.screen_views_in_session THEN sv.screen_view_type END) AS last_screen_view_type

	FROM
		{{.scratch_schema}}.mobile_screen_views_staged{{.entropy}} sv

	GROUP BY 1
	
)