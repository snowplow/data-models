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

CREATE OR REPLACE PROCEDURE {{.scratch_schema}}.sessions_lasts ()
OPTIONS(strict_mode=false)
BEGIN

  CREATE OR REPLACE TABLE {{.scratch_schema}}.sessions_lasts{{.entropy}}
  AS(
    SELECT
      a.domain_sessionid,
      a.page_title AS last_page_title,

      a.page_url AS last_page_url,

      a.page_urlscheme AS last_page_urlscheme,
      a.page_urlhost AS last_page_urlhost,
      a.page_urlpath AS last_page_urlpath,
      a.page_urlquery AS last_page_urlquery,
      a.page_urlfragment AS last_page_urlfragment

    FROM
      {{.scratch_schema}}.page_views_staged{{.entropy}} a

    INNER JOIN {{.scratch_schema}}.sessions_aggregates{{.entropy}} b
    ON a.domain_sessionid = b.domain_sessionid
    -- Don't join on timestamp because people can return to a page after previous page view is complete.
    AND a.page_view_in_session_index = b.page_views
  );
  
  CALL {{.scratch_schema}}.log_model_table('{{.scratch_schema}}.sessions_lasts{{.entropy}}', 'trace', 'sessions');
END;
