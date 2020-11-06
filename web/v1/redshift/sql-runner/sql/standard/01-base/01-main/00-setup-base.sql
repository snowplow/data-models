/*
   Copyright 2020 Snowplow Analytics Ltd. All rights reserved.

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

-- A table storing an identifier for this run of a model - used to identify runs of the model across multiple modules/steps (eg. base, page views share this id per run)
CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.metadata_run_id{{.entropy}} (
    run_id TIMESTAMP
);

-- When base runs, it's always the first module. So it's safe to just truncate here.
TRUNCATE {{.scratch_schema}}.metadata_run_id{{.entropy}};

INSERT INTO {{.scratch_schema}}.metadata_run_id{{.entropy}} (
  SELECT
    GETDATE()
);

-- Permanent metadata table
CREATE TABLE IF NOT EXISTS {{.output_schema}}.web_model_run_metadata{{.entropy}} (
  run_id TIMESTAMP,
  model_version VARCHAR(64),
  module_name VARCHAR(64),
  step_name VARCHAR(64),
  run_start_tstamp TIMESTAMP,
  run_end_tstamp TIMESTAMP,
  rows_this_run INT,
  distinct_key VARCHAR(64),
  distinct_key_count INT,
  time_key VARCHAR(64),
  min_time_key TIMESTAMP,
  max_time_key TIMESTAMP,
  duplicate_rows_removed INT,
  distinct_keys_removed INT
);

-- Setup temp metadata tables for this run
DROP TABLE IF EXISTS {{.scratch_schema}}.base_metadata_this_run{{.entropy}};

CREATE TABLE {{.scratch_schema}}.base_metadata_this_run{{.entropy}} (
  id VARCHAR(64),
  run_id TIMESTAMP,
  model_version VARCHAR(64),
  module_name VARCHAR(64),
  step_name VARCHAR(64),
  run_start_tstamp TIMESTAMP,
  run_end_tstamp TIMESTAMP,
  rows_this_run INT,
  distinct_key VARCHAR(64),
  distinct_key_count INT,
  time_key VARCHAR(64),
  min_time_key TIMESTAMP,
  max_time_key TIMESTAMP,
  duplicate_rows_removed INT,
  distinct_keys_removed INT
);

INSERT INTO {{.scratch_schema}}.base_metadata_this_run{{.entropy}} (
  SELECT
    'run',
    run_id,
    '{{.model_version}}',
    'base',
    'main',
    GETDATE(),
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
  FROM {{.scratch_schema}}.metadata_run_id{{.entropy}}
);


-- Setup manifests
CREATE TABLE IF NOT EXISTS {{.output_schema}}.base_event_id_manifest{{.entropy}} (
  event_id VARCHAR(36),
  collector_tstamp TIMESTAMP
)
DISTSTYLE KEY
DISTKEY (event_id)
SORTKEY (collector_tstamp);

-- Seed table if empty
INSERT INTO {{.output_schema}}.base_event_id_manifest{{.entropy}} (
  SELECT
    'seed'::VARCHAR(36),
    '{{.start_date}}'::TIMESTAMP

  WHERE
    (SELECT collector_tstamp FROM {{.output_schema}}.base_event_id_manifest{{.entropy}} LIMIT 1) IS NULL
    -- ensures that the seed is not re-inserted if the table is populated.
);

CREATE TABLE IF NOT EXISTS {{.output_schema}}.base_session_id_manifest{{.entropy}} (
  session_id VARCHAR(36),
  min_tstamp TIMESTAMP
)
DISTSTYLE KEY
DISTKEY (session_id)
SORTKEY (min_tstamp);

INSERT INTO {{.output_schema}}.base_session_id_manifest{{.entropy}} (
  SELECT
    'seed'::VARCHAR(36),
    '{{.start_date}}'::TIMESTAMP

  WHERE
    (SELECT min_tstamp FROM {{.output_schema}}.base_session_id_manifest{{.entropy}} LIMIT 1) IS NULL
    -- ensures that the seed is not re-inserted if the table is populated.
);

-- Create staged table:
CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.events_staged{{.entropy}} (

	app_id VARCHAR(255) ENCODE ZSTD,
	platform VARCHAR(255) ENCODE ZSTD,
	etl_tstamp TIMESTAMP  ENCODE ZSTD,
	collector_tstamp TIMESTAMP NOT NULL ENCODE RAW,
	dvce_created_tstamp TIMESTAMP ENCODE ZSTD,
	event VARCHAR(128) ENCODE ZSTD,
	event_id CHAR(36) NOT NULL UNIQUE ENCODE ZSTD,
	txn_id INT ENCODE ZSTD,
	name_tracker VARCHAR(128) ENCODE ZSTD,
	v_tracker VARCHAR(100) ENCODE ZSTD,
	v_collector VARCHAR(100) ENCODE ZSTD NOT NULL,
	v_etl VARCHAR(100) ENCODE ZSTD NOT NULL,
	user_id VARCHAR(255) ENCODE ZSTD,
	user_ipaddress VARCHAR(128) ENCODE ZSTD,
	user_fingerprint VARCHAR(128) ENCODE ZSTD,
	domain_userid VARCHAR(128) ENCODE ZSTD,
	domain_sessionidx INT ENCODE ZSTD,
	network_userid VARCHAR(128) ENCODE ZSTD,
	geo_country CHAR(2) ENCODE ZSTD,
	geo_region CHAR(3) ENCODE ZSTD,
	geo_city VARCHAR(75) ENCODE ZSTD,
	geo_zipcode VARCHAR(15) ENCODE ZSTD,
	geo_latitude DOUBLE PRECISION ENCODE ZSTD,
	geo_longitude DOUBLE PRECISION ENCODE ZSTD,
	geo_region_name VARCHAR(100) ENCODE ZSTD,
	ip_isp VARCHAR(100) ENCODE ZSTD,
	ip_organization VARCHAR(128) ENCODE ZSTD,
	ip_domain VARCHAR(128) ENCODE ZSTD,
	ip_netspeed VARCHAR(100) ENCODE ZSTD,
	page_url VARCHAR(4096) ENCODE ZSTD,
	page_title VARCHAR(2000) ENCODE ZSTD,
	page_referrer VARCHAR(4096) ENCODE ZSTD,
	page_urlscheme VARCHAR(16) ENCODE ZSTD,
	page_urlhost VARCHAR(255) ENCODE ZSTD,
	page_urlport INT ENCODE ZSTD,
	page_urlpath VARCHAR(3000) ENCODE ZSTD,
	page_urlquery VARCHAR(6000) ENCODE ZSTD,
	page_urlfragment VARCHAR(3000) ENCODE ZSTD,
	refr_urlscheme VARCHAR(16) ENCODE ZSTD,
	refr_urlhost VARCHAR(255) ENCODE ZSTD,
	refr_urlport INT ENCODE ZSTD,
	refr_urlpath VARCHAR(6000) ENCODE ZSTD,
	refr_urlquery VARCHAR(6000) ENCODE ZSTD,
	refr_urlfragment VARCHAR(3000) ENCODE ZSTD,
	refr_medium VARCHAR(25) ENCODE ZSTD,
	refr_source VARCHAR(50) ENCODE ZSTD,
	refr_term VARCHAR(255) ENCODE ZSTD,
	mkt_medium VARCHAR(255) ENCODE ZSTD,
	mkt_source VARCHAR(255) ENCODE ZSTD,
	mkt_term VARCHAR(255) ENCODE ZSTD,
	mkt_content VARCHAR(500) ENCODE ZSTD,
	mkt_campaign VARCHAR(255) ENCODE ZSTD,
	se_category VARCHAR(1000) ENCODE ZSTD,
	se_action VARCHAR(1000) ENCODE ZSTD,
	se_label VARCHAR(4096) ENCODE ZSTD,
	se_property VARCHAR(1000) ENCODE ZSTD,
	se_value DOUBLE PRECISION ENCODE ZSTD,
	tr_orderid VARCHAR(255) ENCODE ZSTD,
	tr_affiliation VARCHAR(255) ENCODE ZSTD,
	tr_total dec(18,2) ENCODE ZSTD,
	tr_tax dec(18,2) ENCODE ZSTD,
	tr_shipping dec(18,2) ENCODE ZSTD,
	tr_city VARCHAR(255) ENCODE ZSTD,
	tr_state VARCHAR(255) ENCODE ZSTD,
	tr_country VARCHAR(255) ENCODE ZSTD,
	ti_orderid VARCHAR(255) ENCODE ZSTD,
	ti_sku VARCHAR(255) ENCODE ZSTD,
	ti_name VARCHAR(255) ENCODE ZSTD,
	ti_category VARCHAR(255) ENCODE ZSTD,
	ti_price dec(18,2) ENCODE ZSTD,
	ti_quantity INT ENCODE ZSTD,
	pp_xoffset_min INT ENCODE ZSTD,
	pp_xoffset_max INT ENCODE ZSTD,
	pp_yoffset_min INT ENCODE ZSTD,
	pp_yoffset_max INT ENCODE ZSTD,
	useragent VARCHAR(1000) ENCODE ZSTD,
	br_name VARCHAR(50) ENCODE ZSTD,
	br_family VARCHAR(50) ENCODE ZSTD,
	br_version VARCHAR(50) ENCODE ZSTD,
	br_type VARCHAR(50) ENCODE ZSTD,
	br_renderengine VARCHAR(50) ENCODE ZSTD,
	br_lang VARCHAR(255) ENCODE ZSTD,
	br_features_pdf BOOLEAN ENCODE ZSTD,
	br_features_flash BOOLEAN ENCODE ZSTD,
	br_features_java BOOLEAN ENCODE ZSTD,
	br_features_director BOOLEAN ENCODE ZSTD,
	br_features_quicktime BOOLEAN ENCODE ZSTD,
	br_features_realplayer BOOLEAN ENCODE ZSTD,
	br_features_windowsmedia BOOLEAN ENCODE ZSTD,
	br_features_gears BOOLEAN ENCODE ZSTD,
	br_features_silverlight BOOLEAN ENCODE ZSTD,
	br_cookies BOOLEAN ENCODE ZSTD,
	br_colordepth VARCHAR(12) ENCODE ZSTD,
	br_viewwidth INT ENCODE ZSTD,
	br_viewheight INT ENCODE ZSTD,
	os_name VARCHAR(50) ENCODE ZSTD,
	os_family VARCHAR(50)  ENCODE ZSTD,
	os_manufacturer VARCHAR(50)  ENCODE ZSTD,
	os_timezone VARCHAR(255)  ENCODE ZSTD,
	dvce_type VARCHAR(50)  ENCODE ZSTD,
	dvce_ismobile BOOLEAN ENCODE ZSTD,
	dvce_screenwidth INT ENCODE ZSTD,
	dvce_screenheight INT ENCODE ZSTD,
	doc_charset VARCHAR(128) ENCODE ZSTD,
	doc_width INT ENCODE ZSTD,
	doc_height INT ENCODE ZSTD,
	tr_currency CHAR(3) ENCODE ZSTD,
	tr_total_base dec(18, 2) ENCODE ZSTD,
	tr_tax_base dec(18, 2) ENCODE ZSTD,
	tr_shipping_base dec(18, 2) ENCODE ZSTD,
	ti_currency CHAR(3) ENCODE ZSTD,
	ti_price_base dec(18, 2) ENCODE ZSTD,
	base_currency CHAR(3) ENCODE ZSTD,
	geo_timezone VARCHAR(64) ENCODE ZSTD,
	mkt_clickid VARCHAR(128) ENCODE ZSTD,
	mkt_network VARCHAR(64) ENCODE ZSTD,
	etl_tags VARCHAR(500) ENCODE ZSTD,
	dvce_sent_tstamp TIMESTAMP ENCODE ZSTD,
	refr_domain_userid VARCHAR(128) ENCODE ZSTD,
	refr_dvce_tstamp TIMESTAMP ENCODE ZSTD,
	domain_sessionid CHAR(128) ENCODE ZSTD,
	derived_tstamp TIMESTAMP ENCODE ZSTD,
	event_vendor VARCHAR(1000) ENCODE ZSTD,
	event_name VARCHAR(1000) ENCODE ZSTD,
	event_format VARCHAR(128) ENCODE ZSTD,
	event_version VARCHAR(128) ENCODE ZSTD,
	event_fingerprint VARCHAR(128) ENCODE ZSTD,
	true_tstamp TIMESTAMP ENCODE ZSTD,

  page_view_id CHAR(36) ENCODE RAW NOT NULL

)
DISTSTYLE KEY
DISTKEY (event_id)
SORTKEY (collector_tstamp);
