BEGIN;

  DROP TABLE IF EXISTS {{.scratch_schema}}.base_duplicates_this_run{{.entropy}};

  CREATE TABLE {{.scratch_schema}}.base_duplicates_this_run{{.entropy}}
    DISTSTYLE KEY
    DISTKEY (event_id)
    SORTKEY (min_tstamp)
  AS(
    SELECT
      event_id,
      MIN(collector_tstamp) AS min_tstamp,
      count(*) AS num_rows

    FROM
      {{.scratch_schema}}.base_events_this_run_tmp{{.entropy}}

    GROUP BY 1
    HAVING count(*) > 1
  );

  -- Remove duplicates from the table
  DELETE FROM {{.scratch_schema}}.base_events_this_run_tmp{{.entropy}} WHERE event_id IN (SELECT event_id FROM {{.scratch_schema}}.base_duplicates_this_run{{.entropy}});

END;
