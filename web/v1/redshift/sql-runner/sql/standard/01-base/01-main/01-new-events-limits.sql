-- Create a limit for this run - single value table.
DROP TABLE IF EXISTS {{.scratch_schema}}.base_new_events_limits{{.entropy}};

CREATE TABLE {{.scratch_schema}}.base_new_events_limits{{.entropy}} AS(
  SELECT
    DATEADD(HOUR, -{{or .lookback_window 6}}, MAX(collector_tstamp)) AS lower_limit,
    DATEADD(DAY, {{or .update_cadence 7}}, MAX(collector_tstamp)) AS upper_limit,
    DATEADD(DAY, -{{or .session_lookback 365}}, MAX(collector_tstamp))  AS session_limit

  FROM
    {{.output_schema}}.base_event_id_manifest{{.entropy}}
);
