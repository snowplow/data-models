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

BEGIN

  {{if ne (or .skip_derived false) true}}

    -- Commit to production if enabled
    -- Note: Automigrate hardcoded to false as all columns are to be explicitly defined in model.
    CALL {{.output_schema}}.commit_table('{{.scratch_schema}}',               -- sourceDataset
                                         'mobile_users_this_run{{.entropy}}', -- sourceTable
                                         '{{.output_schema}}',                -- targetDataset
                                         'mobile_users{{.entropy}}',          -- targetTable
                                         'device_user_id',                    -- joinKey
                                         'start_tstamp',                      -- partitionKey
                                         FALSE);                              -- automigrate

  {{end}}

  -- Commit metadata
  INSERT INTO {{.output_schema}}.datamodel_metadata{{.entropy}} (
    SELECT
      run_id,
      model_version,
      model,
      module,
      run_start_tstamp,
      CURRENT_TIMESTAMP() AS run_end_tstamp,
      rows_this_run,
      distinct_key,
      distinct_key_count,
      time_key,
      min_time_key,
      max_time_key,
      duplicate_rows_removed,
      distinct_keys_removed
    FROM {{.scratch_schema}}.mobile_users_metadata_this_run{{.entropy}}
  );

END;
