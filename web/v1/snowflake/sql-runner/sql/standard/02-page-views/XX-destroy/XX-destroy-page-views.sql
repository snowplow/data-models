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


DROP TABLE IF EXISTS {{.output_schema}}.page_views{{.entropy}};
DROP TABLE IF EXISTS {{.scratch_schema}}.page_views_staged{{.entropy}};

-- Snowflake supports overloading of procedure names.
-- So the data types of the arguments are needed to identify the procedure to drop.
DROP PROCEDURE IF EXISTS {{.output_schema}}.mk_transaction(VARCHAR);

DROP PROCEDURE IF EXISTS {{.output_schema}}.column_check(VARCHAR,
                                                         VARCHAR,
                                                         VARCHAR,
                                                         VARCHAR,
                                                         VARCHAR);

DROP PROCEDURE IF EXISTS {{.output_schema}}.commit_staged(VARCHAR,VARCHAR,VARCHAR,VARCHAR);
