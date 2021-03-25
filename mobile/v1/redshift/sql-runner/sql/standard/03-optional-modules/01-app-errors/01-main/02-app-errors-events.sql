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
{{if eq (or .enabled false) true}}

  DROP TABLE IF EXISTS {{.scratch_schema}}.mobile_app_errors_events{{.entropy}};

  CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.mobile_app_errors_events{{.entropy}} (
    root_id CHAR(36),
    root_tstamp TIMESTAMP ENCODE ZSTD,
    message VARCHAR(2048) ENCODE ZSTD,
    programming_language VARCHAR(12) ENCODE ZSTD,
    class_name VARCHAR(1024) ENCODE ZSTD,
    exception_name VARCHAR(1024) ENCODE ZSTD,
    is_fatal BOOLEAN ENCODE ZSTD,
    line_number INT ENCODE ZSTD,
    stack_trace VARCHAR(8192) ENCODE ZSTD,
    thread_id INT ENCODE ZSTD,
    thread_name VARCHAR(1024) ENCODE ZSTD
  )
  DISTSTYLE KEY
  DISTKEY (root_id)
  SORTKEY (root_tstamp);

  INSERT INTO {{.scratch_schema}}.mobile_app_errors_events{{.entropy}} (
    SELECT
      ae.root_id,
      ae.root_tstamp,
      ae.message,
      ae.programming_language,
      ae.class_name,
      ae.exception_name,
      ae.is_fatal,
      ae.line_number,
      ae.stack_trace,
      ae.thread_id,
      ae.thread_name

    FROM {{.input_schema}}.com_snowplowanalytics_snowplow_application_error_1 ae

    WHERE ae.root_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.mobile_app_errors_run_limits{{.entropy}})
      AND ae.root_tstamp <= (SELECT upper_limit FROM {{.scratch_schema}}.mobile_app_errors_run_limits{{.entropy}})
  );

{{else}}

  SELECT 1;

{{end}}
