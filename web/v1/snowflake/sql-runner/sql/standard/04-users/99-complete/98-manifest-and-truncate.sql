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


-- Update manifest and truncate input table just processed
CALL {{.output_schema}}.mk_transaction(
  '
  DELETE FROM {{.output_schema}}.users_manifest{{.entropy}}
    WHERE
      domain_userid IN (SELECT domain_userid FROM {{.scratch_schema}}.users_userids_this_run{{.entropy}});

  INSERT INTO {{.output_schema}}.users_manifest{{.entropy}}
    SELECT * FROM {{.scratch_schema}}.users_userids_this_run{{.entropy}};

  TRUNCATE TABLE {{.scratch_schema}}.sessions_userid_manifest_staged{{.entropy}};
  '
);
