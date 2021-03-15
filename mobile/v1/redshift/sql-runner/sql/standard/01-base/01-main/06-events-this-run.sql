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

DROP TABLE IF EXISTS {{.scratch_schema}}.{{.model}}_base_events_this_run_tmp{{.entropy}};

CREATE TABLE {{.scratch_schema}}.{{.model}}_base_events_this_run_tmp{{.entropy}}
  DISTSTYLE KEY
  DISTKEY (event_id)
  SORTKEY (collector_tstamp)
AS(
  {{if eq .model "web"}}

  SELECT
      a.*,
      DENSE_RANK() OVER (PARTITION BY a.event_id ORDER BY a.collector_tstamp) AS event_id_dedupe_index --dense_rank to catch event_ids with dupe tstamps later

  FROM
    {{.input_schema}}.events a
  INNER JOIN
    {{.scratch_schema}}.{{.model}}_base_sessions_to_include{{.entropy}} b
  ON a.domain_sessionid = b.session_id

  WHERE
    a.collector_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.{{.model}}_base_run_limits{{.entropy}})
    AND a.collector_tstamp <= (SELECT upper_limit FROM {{.scratch_schema}}.{{.model}}_base_run_limits{{.entropy}})
    AND a.platform IN (
      {{range $i, $platform := .platform_filters}} {{if $i}}, {{end}} '{{$platform}}' {{else}} 'web' {{end}}
      )
    {{if .app_id_filters}}
    -- Filter by app_id. Ignore if not specified. 
    AND a.app_id IN (
      {{range $i, $app_id := .app_id_filters}} {{if $i}}, {{end}} '{{$app_id}}' {{end}}
      )
    {{end}}
  
  {{end}}

  {{if eq .model "mobile"}}
  -- Could split mobile_session_ids out into a scratch table.
  WITH mobile_session_ids AS (
    SELECT
      s.root_id,
      s.root_tstamp,
      s.session_id,
      s.session_index,
      s.previous_session_id,
      s.user_id,
      s.first_event_id,
      ROW_NUMBER() OVER(PARTITION BY s.root_id, s.root_tstamp) AS row_num

    FROM
      {{.input_schema}}.com_snowplowanalytics_snowplow_client_session_1 AS s

    WHERE
      s.root_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.{{.model}}_base_run_limits{{.entropy}})
      AND s.root_tstamp <= (SELECT upper_limit FROM {{.scratch_schema}}.{{.model}}_base_run_limits{{.entropy}})
    )

    SELECT
      a.app_id,
      a.platform,
      a.etl_tstamp,
      a.collector_tstamp,
      a.dvce_created_tstamp,
      a.event,
      a.event_id,
      a.name_tracker,
      a.v_tracker,
      a.v_collector,
      a.v_etl,
      a.user_id,
      a.user_ipaddress,
      a.network_userid,
      a.geo_country,
      a.geo_region,
      a.geo_city,
      a.geo_zipcode,
      a.geo_latitude,
      a.geo_longitude,
      a.geo_region_name,
      a.ip_isp,
      a.ip_organization,
      a.ip_domain,
      a.ip_netspeed,
      a.se_category,
      a.se_action,
      a.se_label,
      a.se_property,
      a.se_value,
      a.tr_orderid,
      a.tr_affiliation,
      a.tr_total,
      a.tr_tax,
      a.tr_shipping,
      a.tr_city,
      a.tr_state,
      a.tr_country,
      a.ti_orderid,
      a.ti_sku,
      a.ti_name,
      a.ti_category,
      a.ti_price,
      a.ti_quantity,
      a.useragent,
      a.dvce_screenwidth,
      a.dvce_screenheight,
      a.tr_currency,
      a.tr_total_base,
      a.tr_tax_base,
      a.tr_shipping_base,
      a.ti_currency,
      a.ti_price_base,
      a.base_currency,
      a.geo_timezone,
      a.etl_tags,
      a.dvce_sent_tstamp,
      a.derived_tstamp,
      a.event_vendor,
      a.event_name,
      a.event_format,
      a.event_version,
      a.event_fingerprint,
      a.true_tstamp,
      mob_session.session_id,
      mob_session.session_index,
      mob_session.previous_session_id,
      mob_session.user_id AS device_user_id,
      mob_session.first_event_id AS session_first_event_id,
      DENSE_RANK() OVER (PARTITION BY a.event_id ORDER BY a.collector_tstamp) AS event_id_dedupe_index --dense_rank to catch event_ids with dupe tstamps later

    FROM
      {{.input_schema}}.events a

    INNER JOIN
      mobile_session_ids AS mob_session
      ON a.event_id = mob_session.root_id
      AND a.collector_tstamp = mob_session.root_tstamp
      AND mob_session.row_num = 1 --avoid fan

    INNER JOIN
      {{.scratch_schema}}.{{.model}}_base_sessions_to_include{{.entropy}} b
      ON mob_session.session_id = b.session_id
    
    WHERE
      a.collector_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.{{.model}}_base_run_limits{{.entropy}})
      AND a.collector_tstamp <= (SELECT upper_limit FROM {{.scratch_schema}}.{{.model}}_base_run_limits{{.entropy}})
      AND a.platform IN (
        {{range $i, $platform := .platform_filters}} {{if $i}}, {{end}} '{{$platform}}' {{else}} 'mob' {{end}}
        )
      {{if .app_id_filters}}
      -- Filter by app_id. Ignore if not specified. 
      AND a.app_id IN (
        {{range $i, $app_id := .app_id_filters}} {{if $i}}, {{end}} '{{$app_id}}' {{end}}
        )
      {{end}}

  {{end}}

);

-- Create staged event ID table before deduplication, for an accurate manifest.
DROP TABLE IF EXISTS {{.scratch_schema}}.{{.model}}_base_event_ids_this_run{{.entropy}};

CREATE TABLE {{.scratch_schema}}.{{.model}}_base_event_ids_this_run{{.entropy}} AS(
  --DISTINCT to avoid fan when joined on sessions_to_process?
  SELECT
    event_id,
    collector_tstamp
  FROM {{.scratch_schema}}.{{.model}}_base_events_this_run_tmp{{.entropy}}
);
