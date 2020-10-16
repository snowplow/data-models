DROP TABLE IF EXISTS {{.scratch_schema}}.events_this_run{{.entropy}};

CREATE TABLE {{.scratch_schema}}.events_this_run{{.entropy}}
  DISTSTYLE KEY
  DISTKEY (event_id)
  SORTKEY (collector_tstamp)
AS(
  SELECT
      a.*,
      b.id AS page_view_id

  FROM
    {{.scratch_schema}}.base_events_this_run_tmp{{.entropy}} a
  LEFT JOIN
    (
    SELECT
      root_id,
      root_tstamp,
      id
    FROM {{.input_schema}}.com_snowplowanalytics_snowplow_web_page_1
    WHERE root_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.base_run_limits{{.entropy}})
    AND   root_tstamp <= (SELECT upper_limit FROM {{.scratch_schema}}.base_run_limits{{.entropy}})
    ) b
    -- We deviate in style here in the name of performance.
  ON a.event_id = b.root_id
  AND a.collector_tstamp = b.root_tstamp
);
