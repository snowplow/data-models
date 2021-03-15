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
DROP TABLE IF EXISTS {{.scratch_schema}}.{{.model}}_events_this_run{{.entropy}};

CREATE TABLE {{.scratch_schema}}.{{.model}}_events_this_run{{.entropy}}
  DISTSTYLE KEY
  DISTKEY (event_id)
  SORTKEY (collector_tstamp)
AS(

{{if eq .model "web"}} 

  SELECT
    a.app_id,
    a.platform,
    a.etl_tstamp,
    a.collector_tstamp,
    a.dvce_created_tstamp,
    a.event,
    a.event_id,
    a.txn_id,
    a.name_tracker,
    a.v_tracker,
    a.v_collector,
    a.v_etl,
    a.user_id,
    a.user_ipaddress,
    a.user_fingerprint,
    a.domain_userid,
    a.domain_sessionidx,
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
    a.page_url,
    a.page_title,
    a.page_referrer,
    a.page_urlscheme,
    a.page_urlhost,
    a.page_urlport,
    a.page_urlpath,
    a.page_urlquery,
    a.page_urlfragment,
    a.refr_urlscheme,
    a.refr_urlhost,
    a.refr_urlport,
    a.refr_urlpath,
    a.refr_urlquery,
    a.refr_urlfragment,
    a.refr_medium,
    a.refr_source,
    a.refr_term,
    a.mkt_medium,
    a.mkt_source,
    a.mkt_term,
    a.mkt_content,
    a.mkt_campaign,
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
    a.pp_xoffset_min,
    a.pp_xoffset_max,
    a.pp_yoffset_min,
    a.pp_yoffset_max,
    a.useragent,
    a.br_name,
    a.br_family,
    a.br_version,
    a.br_type,
    a.br_renderengine,
    a.br_lang,
    a.br_features_pdf,
    a.br_features_flash,
    a.br_features_java,
    a.br_features_director,
    a.br_features_quicktime,
    a.br_features_realplayer,
    a.br_features_windowsmedia,
    a.br_features_gears,
    a.br_features_silverlight,
    a.br_cookies,
    a.br_colordepth,
    a.br_viewwidth,
    a.br_viewheight,
    a.os_name,
    a.os_family,
    a.os_manufacturer,
    a.os_timezone,
    a.dvce_type,
    a.dvce_ismobile,
    a.dvce_screenwidth,
    a.dvce_screenheight,
    a.doc_charset,
    a.doc_width,
    a.doc_height,
    a.tr_currency,
    a.tr_total_base,
    a.tr_tax_base,
    a.tr_shipping_base,
    a.ti_currency,
    a.ti_price_base,
    a.base_currency,
    a.geo_timezone,
    a.mkt_clickid,
    a.mkt_network,
    a.etl_tags,
    a.dvce_sent_tstamp,
    a.refr_domain_userid,
    a.refr_dvce_tstamp,
    a.domain_sessionid,
    a.derived_tstamp,
    a.event_vendor,
    a.event_name,
    a.event_format,
    a.event_version,
    a.event_fingerprint,
    a.true_tstamp,
    b.id AS page_view_id

  FROM
    {{.scratch_schema}}.{{.model}}_base_events_this_run_tmp{{.entropy}} a
  LEFT JOIN
    {{.scratch_schema}}.web_events_addon_pv_context{{.entropy}} b
  ON a.event_id = b.root_id
  AND a.collector_tstamp = b.root_tstamp

  WHERE
    a.event_id_dedupe_index = 1
  
{{end}}

{{if eq .model "mobile"}} 

  SELECT
    events.app_id,
    events.platform,
    events.etl_tstamp,
    events.collector_tstamp,
    events.dvce_created_tstamp,
    events.event,
    events.event_id,
    events.name_tracker,
    events.v_tracker,
    events.v_collector,
    events.v_etl,
    events.user_id,
    events.user_ipaddress,
    events.network_userid,
    events.geo_country,
    events.geo_region,
    events.geo_city,
    events.geo_zipcode,
    events.geo_latitude,
    events.geo_longitude,
    events.geo_region_name,
    events.ip_isp,
    events.ip_organization,
    events.ip_domain,
    events.ip_netspeed,
    events.se_category,
    events.se_action,
    events.se_label,
    events.se_property,
    events.se_value,
    events.tr_orderid,
    events.tr_affiliation,
    events.tr_total,
    events.tr_tax,
    events.tr_shipping,
    events.tr_city,
    events.tr_state,
    events.tr_country,
    events.ti_orderid,
    events.ti_sku,
    events.ti_name,
    events.ti_category,
    events.ti_price,
    events.ti_quantity,
    events.useragent,
    events.dvce_screenwidth,
    events.dvce_screenheight,
    events.tr_currency,
    events.tr_total_base,
    events.tr_tax_base,
    events.tr_shipping_base,
    events.ti_currency,
    events.ti_price_base,
    events.base_currency,
    events.geo_timezone,
    events.etl_tags,
    events.dvce_sent_tstamp,
    events.derived_tstamp,
    events.event_vendor,
    events.event_name,
    events.event_format,
    events.event_version,
    events.event_fingerprint,
    events.event_vendor || '/' || events.event_name || '/' || events.event_format || '/' || events.event_version AS event_schema,
    events.true_tstamp,
    events.session_id,
    events.session_index,
    events.previous_session_id,
    events.device_user_id,
    events.session_first_event_id,
    screen_context.screen_id,
    screen_context.screen_name,
    screen_context.screen_activity,
    screen_context.screen_fragment,
    screen_context.screen_top_view_controller,
    screen_context.screen_type,
    screen_context.screen_view_controller,
    mob_context.device_manufacturer,
    mob_context.device_model,
    mob_context.os_type,
    mob_context.os_version,
    mob_context.android_idfa,
    mob_context.apple_idfa,
    mob_context.apple_idfv,
    mob_context.carrier,
    mob_context.open_idfa,
    mob_context.network_technology,
    mob_context.network_type,
    geo_context.device_latitude,
    geo_context.device_longitude,
    geo_context.device_latitude_longitude_accuracy,
    geo_context.device_altitude,
    geo_context.device_altitude_accuracy,
    geo_context.device_bearing,
    geo_context.device_speed,
    app_context.build,
    app_context.version,
    ROW_NUMBER() OVER(PARTITION BY events.session_id ORDER BY events.derived_tstamp) AS event_index_in_session

  FROM
    {{.scratch_schema}}.{{.model}}_base_events_this_run_tmp{{.entropy}} events
  LEFT JOIN
    {{.scratch_schema}}.mobile_events_addon_screen_context{{.entropy}} AS screen_context
  ON events.event_id = screen_context.root_id
  AND events.collector_tstamp = screen_context.root_tstamp 
  LEFT JOIN
    {{.scratch_schema}}.mobile_events_addon_mobile_context{{.entropy}} AS mob_context
  ON events.event_id = mob_context.root_id
  AND events.collector_tstamp = mob_context.root_tstamp
  LEFT JOIN
    {{.scratch_schema}}.mobile_events_addon_geolocation_context{{.entropy}} AS geo_context
  ON events.event_id = geo_context.root_id
  AND events.collector_tstamp = geo_context.root_tstamp
  LEFT JOIN
    {{.scratch_schema}}.mobile_events_addon_application_context{{.entropy}} AS app_context
  ON events.event_id = app_context.root_id
  AND events.collector_tstamp = app_context.root_tstamp
  
  WHERE
    events.event_id_dedupe_index = 1
{{end}}
);
