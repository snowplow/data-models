-- Subset the manifest for performance.
DROP TABLE IF EXISTS {{.scratch_schema}}.base_run_manifest{{.entropy}};

CREATE TABLE {{.scratch_schema}}.base_run_manifest{{.entropy}}
  DISTSTYLE KEY
  DISTKEY (event_id)
  SORTKEY (collector_tstamp)
AS(
  SELECT
    *

  FROM
    {{.output_schema}}.base_event_id_manifest{{.entropy}}

  WHERE
    collector_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.base_new_events_limits{{.entropy}})
);

DROP TABLE IF EXISTS {{.scratch_schema}}.base_session_id_run_manifest{{.entropy}};

-- subset session manifest table - should be as long a timeframe as practical
CREATE TABLE {{.scratch_schema}}.base_session_id_run_manifest{{.entropy}}
  DISTSTYLE KEY
  DISTKEY (session_id)
  SORTKEY (min_tstamp)
AS(
  SELECT
    *

  FROM
    {{.output_schema}}.base_session_id_manifest{{.entropy}}

  WHERE
    min_tstamp >= (SELECT session_limit FROM {{.scratch_schema}}.base_new_events_limits{{.entropy}})
);
