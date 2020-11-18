CREATE OR REPLACE TABLE {{.scratch_schema}}.events_staged{{.entropy}} AS(
  SELECT
    *,
    contexts_com_snowplowanalytics_snowplow_web_page_1_0_0[OFFSET(0)].id AS page_view_id

  FROM {{.input_schema}}.events
  WHERE collector_tstamp >= TIMESTAMP(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY))
  AND app_id = 'website' -- Filter out our test data which has duplicates up to millions of times...
);