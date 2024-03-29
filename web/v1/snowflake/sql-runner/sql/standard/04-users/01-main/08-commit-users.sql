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


-- production tables
{{if ne (or .skip_derived false) true}}

  CALL {{.output_schema}}.mk_transaction(
    '
    DELETE FROM {{.output_schema}}.users{{.entropy}}
      WHERE domain_userid IN (SELECT domain_userid FROM {{.scratch_schema}}.users_this_run{{.entropy}})
        AND start_tstamp >= (SELECT TIMEADD(DAY, -{{or .upsert_lookback_days 30}}, MIN(start_tstamp)) FROM {{.scratch_schema}}.users_this_run{{.entropy}});

    INSERT INTO {{.output_schema}}.users{{.entropy}}
      SELECT * FROM {{.scratch_schema}}.users_this_run{{.entropy}};
    '
  );

{{end}}

-- commit metadata
INSERT INTO {{.output_schema}}.datamodel_metadata{{.entropy}}
  SELECT
    run_id,
    model_version,
    model,
    module,
    run_start_tstamp,
    CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS run_end_tstamp,
    rows_this_run,
    distinct_key,
    distinct_key_count,
    time_key,
    min_time_key,
    max_time_key,
    duplicate_rows_removed,
    distinct_keys_removed
  FROM
    {{.scratch_schema}}.users_metadata_this_run{{.entropy}};
