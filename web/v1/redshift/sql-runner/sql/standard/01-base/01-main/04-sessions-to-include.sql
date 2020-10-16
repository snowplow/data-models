-- Get only those session ids that we'd like to process in this run.
DROP TABLE IF EXISTS {{.scratch_schema}}.base_sessions_to_include{{.entropy}};

CREATE TABLE {{.scratch_schema}}.base_sessions_to_include{{.entropy}}
  DISTSTYLE KEY
  DISTKEY (session_id)
  SORTKEY (session_id)
AS(
  SELECT
    a.session_id,
    LEAST(a.min_tstamp, b.min_tstamp) AS min_tstamp

  FROM
    {{.scratch_schema}}.base_sessions_to_process{{.entropy}} a
  LEFT JOIN
    {{.scratch_schema}}.base_session_id_run_manifest{{.entropy}} b
    ON a.session_id = b.session_id

  WHERE
    a.session_id IS NOT NULL
    AND DATEDIFF(DAY, NVL(b.min_tstamp, a.max_tstamp), a.max_tstamp) <= {{or .days_late_allowed 3}}
    -- Compares the max_tstamp of new data to the min_tstamp for its existing session, if one exists.

);
