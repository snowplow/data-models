-- 1. Call the base model
CALL scratch_dev1.base_model('2020-01-01',  -- start_date
                              6,            -- lookback_window_hours
                              1,            -- update_cadence_days
                              365,          -- session_lookback_days
                              3,            -- days_late_allowed
                              FALSE,        -- skip_derived
                              TRUE);        -- stage_next

-- 2. Call the page views model
CALL scratch_dev1.page_views_model (10,     -- heartbeat
                                    5,      -- minimumVisitLength
                                    false,  -- iab
                                    false,  -- ua_parser
                                    false,  -- yauaa
                                    false,  -- skip_derived
                                    true);  -- stage_next

-- 3. Call the sessions model
CALL scratch_dev1.sessions_model(false,     -- skip_derived
                                 true);     -- stage_next


-- 4. Call the users model
CALL scratch_dev1.users_model(false);       -- skip_derived

-- 5. Write my own custom logic
CREATE OR REPLACE TABLE scratch_dev1.link_clicks AS(
  WITH click_count AS(
    SELECT
      page_view_id,
      COUNT(DISTINCT event_id) AS link_clicks,

    FROM
      scratch_dev1.events_staged
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

    FROM scratch_dev1.events_staged
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



-- 6. Complete the incremental logic, and clean up
CALL scratch_dev1.base_complete('trace',          -- cleanup level
                                false);           -- ends_run

CALL scratch_dev1.page_views_complete('trace',   -- cleanup level
                                        false);   -- ends_run
                                        
                                        
CALL scratch_dev1.sessions_complete('trace',      -- cleanup level
                                    false);       -- ends_run

CALL scratch_dev1.users_complete('trace');       -- cleanup level
