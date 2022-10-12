/*
   Copyright 2021-2022 Snowplow Analytics Ltd. All rights reserved.

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


CREATE OR REPLACE PROCEDURE {{.scratch_schema}}.create_events_this_run()
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
  AS
  $$

  var sql_stmt = `
      SELECT listagg(isc.column_name, ',') WITHIN GROUP (order by isc.ordinal_position)
      FROM information_schema.columns AS isc
      WHERE table_schema=UPPER('{{.input_schema}}')
        AND table_name=UPPER('events')
        AND column_name != UPPER('contexts_com_snowplowanalytics_snowplow_web_page_1');`;

  var res = snowflake.createStatement({sqlText: sql_stmt}).execute();
  res.next();
  var result = res.getColumnValue(1);

  var new_col = 'contexts_com_snowplowanalytics_snowplow_web_page_1[0]:id::varchar(36) AS page_view_id';
  if (result !== '') {
      new_col = new_col + ',';
  }

  var sql_stmt2 =`SET (LOWER_LIMIT, UPPER_LIMIT) = (SELECT lower_limit, upper_limit FROM {{.scratch_schema}}.base_run_limits{{.entropy}});`
  snowflake.createStatement({sqlText: sql_stmt2}).execute();

  var fin_query=`

    CREATE OR REPLACE TABLE {{.scratch_schema}}.events_this_run{{.entropy}}
    AS
      SELECT
        ` + new_col + ` ` + result + `
      FROM {{.input_schema}}.events AS a
      INNER JOIN {{.scratch_schema}}.base_sessions_to_include{{.entropy}} AS b
        ON a.domain_sessionid = b.session_id
      WHERE a.collector_tstamp >= $LOWER_LIMIT
        AND a.collector_tstamp <= $UPPER_LIMIT;`;

  snowflake.createStatement({sqlText: fin_query}).execute();

  return 'ok. create_events_this_run succeeded.';

  $$
;

CALL {{.scratch_schema}}.create_events_this_run();

-- Create staged event ID table before deduplication, for an accurate manifest.
CREATE OR REPLACE TABLE {{.scratch_schema}}.base_event_ids_this_run{{.entropy}}
AS (
  SELECT
    event_id,
    collector_tstamp

  FROM
    {{.scratch_schema}}.events_this_run{{.entropy}}
);
