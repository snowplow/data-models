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

BEGIN;

  DROP TABLE IF EXISTS {{.scratch_schema}}.{{.model}}_base_duplicates_this_run{{.entropy}};

  CREATE TABLE {{.scratch_schema}}.{{.model}}_base_duplicates_this_run{{.entropy}}
    DISTSTYLE KEY
    DISTKEY (event_id)
    SORTKEY (min_tstamp)
  AS(
    SELECT
      event_id,
      MIN(collector_tstamp) AS min_tstamp,
      count(*) AS num_rows

    FROM
      {{.scratch_schema}}.{{.model}}_base_events_this_run_tmp{{.entropy}}

    WHERE
      event_id_dedupe_index = 1 --take earliest collector_tstamp per event_id
    GROUP BY 1
    HAVING count(*) > 1 --Only remove rows which have duplicated collector_tstamp
  );

  -- Remove duplicates from the table
  DELETE FROM {{.scratch_schema}}.{{.model}}_base_events_this_run_tmp{{.entropy}} 
  WHERE event_id IN (SELECT event_id FROM {{.scratch_schema}}.{{.model}}_base_duplicates_this_run{{.entropy}});

END;
