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

DROP TABLE IF EXISTS {{.output_schema}}.mobile_users{{.entropy}};
DROP TABLE IF EXISTS {{.output_schema}}.mobile_users_manifest{{.entropy}};
DROP FUNCTION IF EXISTS {{.output_schema}}.columnCheckQuery;
DROP PROCEDURE IF EXISTS {{.output_schema}}.commit_table;
DROP PROCEDURE IF EXISTS {{.output_schema}}.combine_column_versions;
DROP PROCEDURE IF EXISTS {{.output_schema}}.mobile_app_errors_fields;
DROP PROCEDURE IF EXISTS {{.output_schema}}.mobile_mobile_context_fields;
DROP PROCEDURE IF EXISTS {{.output_schema}}.mobile_session_context_fields;
