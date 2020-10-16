DROP TABLE IF EXISTS {{.scratch_schema}}.base_events_this_run_tmp{{.entropy}};

CREATE TABLE {{.scratch_schema}}.base_events_this_run_tmp{{.entropy}}
  DISTSTYLE KEY
  DISTKEY (event_id)
  SORTKEY (collector_tstamp)
AS(
  SELECT
      a.*

  FROM
    {{.input_schema}}.events a
  INNER JOIN
    {{.scratch_schema}}.base_sessions_to_include{{.entropy}} b
  ON a.domain_sessionid = b.session_id

  WHERE
    a.collector_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.base_run_limits{{.entropy}})
    AND a.collector_tstamp <= (SELECT upper_limit FROM {{.scratch_schema}}.base_run_limits{{.entropy}})
);

-- Create staged event ID table before deduplication, for an accurate manifest.
DROP TABLE IF EXISTS {{.scratch_schema}}.base_event_ids_this_run{{.entropy}};

CREATE TABLE {{.scratch_schema}}.base_event_ids_this_run{{.entropy}} AS(
  SELECT
    event_id,
    collector_tstamp
  FROM {{.scratch_schema}}.base_events_this_run_tmp{{.entropy}}
);
