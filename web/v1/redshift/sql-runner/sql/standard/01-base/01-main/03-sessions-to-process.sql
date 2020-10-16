-- Get sessionids for new events
DROP TABLE IF EXISTS {{.scratch_schema}}.base_sessions_to_process{{.entropy}};

CREATE TABLE {{.scratch_schema}}.base_sessions_to_process{{.entropy}}
  DISTSTYLE KEY
  DISTKEY (session_id)
  SORTKEY (session_id)
AS(
  SELECT
    domain_sessionid AS session_id,
    MIN(a.collector_tstamp) AS min_tstamp,
    MAX(a.collector_tstamp) AS max_tstamp

  FROM
    {{.input_schema}}.events a
  LEFT JOIN
    {{.scratch_schema}}.base_run_manifest{{.entropy}} b
    ON a.event_id = b.event_id
    AND a.collector_tstamp = b.collector_tstamp

  WHERE
    b.event_id IS NULL
    AND a.collector_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.base_new_events_limits{{.entropy}})
    AND a.collector_tstamp <= (SELECT upper_limit FROM {{.scratch_schema}}.base_new_events_limits{{.entropy}})
    AND a.domain_sessionid IS NOT NULL
    AND DATEDIFF(DAY, a.dvce_created_tstamp, a.dvce_sent_tstamp) <= {{or .days_late_allowed 3}}
    -- don't process data that's too late
  GROUP BY 1
);
