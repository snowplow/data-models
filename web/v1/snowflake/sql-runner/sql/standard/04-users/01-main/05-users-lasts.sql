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


CREATE OR REPLACE TABLE {{.scratch_schema}}.users_lasts{{.entropy}}
AS (
  SELECT
    a.domain_userid,

    a.last_page_title,
    a.last_page_url,
    a.last_page_urlscheme,
    a.last_page_urlhost,
    a.last_page_urlpath,
    a.last_page_urlquery,
    a.last_page_urlfragment

  FROM
    {{.scratch_schema}}.users_sessions_this_run{{.entropy}} AS a

  INNER JOIN {{.scratch_schema}}.users_aggregates{{.entropy}} AS b
    ON a.domain_userid = b.domain_userid AND a.end_tstamp = b.end_tstamp
);
