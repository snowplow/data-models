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


CREATE TABLE IF NOT EXISTS {{.output_schema}}.page_views_join{{.entropy}} (

  page_view_id                 VARCHAR(36)      NOT NULL,
  start_tstamp                 TIMESTAMP_NTZ,
  link_clicks                  INTEGER,
  first_link_target            VARCHAR,
  bounced_page_view            BOOLEAN,
  engagement_score             DOUBLE PRECISION,
  channel                      VARCHAR(255)
);
