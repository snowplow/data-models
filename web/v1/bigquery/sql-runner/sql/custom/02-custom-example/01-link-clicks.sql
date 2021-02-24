-- We use entropy templates to allow ourselves easier separation of dev and prod runs.
CREATE OR REPLACE TABLE {{.scratch_schema}}.link_clicks{{.entropy}} AS(
  WITH click_count AS(
    SELECT
      page_view_id,
      COUNT(DISTINCT event_id) AS link_clicks,

    FROM
      {{.scratch_schema}}.events_staged{{.entropy}}
    WHERE
      event_name = 'link_click'
    GROUP BY 1

  ), first_and_last AS(
    SELECT
      page_view_id,

      FIRST_VALUE(unstruct_event_com_snowplowanalytics_snowplow_link_click_1_0_1.target_url) OVER(PARTITION BY page_view_id
         ORDER BY derived_tstamp desc
         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_link_clicked,

      LAST_VALUE(unstruct_event_com_snowplowanalytics_snowplow_link_click_1_0_1.target_url) OVER(PARTITION BY page_view_id
         ORDER BY derived_tstamp desc
         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_link_clicked

    FROM {{.scratch_schema}}.events_staged{{.entropy}}
    WHERE event_name = 'page_view'
  )
  SELECT
    b.page_view_id,
    COALESCE(a.link_clicks, 0) AS link_clicks,
    b.first_link_clicked,
    b.last_link_clicked

  FROM
    click_count a
  LEFT JOIN
    first_and_last b
  ON b.page_view_id = a.page_view_id
);
