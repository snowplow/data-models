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

CREATE OR REPLACE PROCEDURE {{.scratch_schema}}.setup_users (start_date STRING)
OPTIONS(strict_mode=false)
BEGIN

  CREATE TABLE IF NOT EXISTS {{.output_schema}}.users_manifest{{.entropy}}
  PARTITION BY DATE(start_tstamp)
  CLUSTER BY domain_userid
  AS(
    SELECT
      'seed' AS domain_userid,
      TIMESTAMP(start_date) AS start_tstamp
  );

  CREATE TABLE IF NOT EXISTS {{.output_schema}}.users{{.entropy}} (
    -- user fields
    user_id STRING,
    domain_userid STRING,
    network_userid STRING,

    start_tstamp TIMESTAMP,
    end_tstamp TIMESTAMP,

    page_views INT64,

    sessions INT64,

    engaged_time_in_s INT64,

    -- first page fields
    first_page_title STRING,

    first_page_url STRING,

    first_page_urlscheme STRING,
    first_page_urlhost STRING,
    first_page_urlpath STRING,
    first_page_urlquery STRING,
    first_page_urlfragment STRING,

    last_page_title STRING,

    last_page_url STRING,

    last_page_urlscheme STRING,
    last_page_urlhost STRING,
    last_page_urlpath STRING,
    last_page_urlquery STRING,
    last_page_urlfragment STRING,

    -- referrer fields
    referrer STRING,

    refr_urlscheme STRING,
    refr_urlhost STRING,
    refr_urlpath STRING,
    refr_urlquery STRING,
    refr_urlfragment STRING,

    refr_medium STRING,
    refr_source STRING,
    refr_term STRING,

    -- marketing fields
    mkt_medium STRING,
    mkt_source STRING,
    mkt_term STRING,
    mkt_content STRING,
    mkt_campaign STRING,
    mkt_clickid STRING,
    mkt_network STRING

  )
  PARTITION BY DATE(start_tstamp)
  CLUSTER BY user_id,domain_userid;
  
  CALL {{.scratch_schema}}.log_model_table('{{.output_schema}}.users_manifest{{.entropy}}', 'prod', 'users');
  CALL {{.scratch_schema}}.log_model_table('{{.output_schema}}.users{{.entropy}}', 'prod', 'users');
END;
