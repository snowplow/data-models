BEGIN;

  DROP TABLE IF EXISTS {{.scratch_schema}}.pv_page_view_id_duplicates_this_run{{.entropy}};

  CREATE TABLE {{.scratch_schema}}.pv_page_view_id_duplicates_this_run{{.entropy}}
    DISTSTYLE KEY
    DISTKEY (page_view_id)
    SORTKEY (page_view_id)
  AS(
    SELECT
      page_view_id,
      count(*) AS num_rows,
      count(DISTINCT event_id) AS dist_event_ids

    FROM
      {{.scratch_schema}}.pv_page_view_events{{.entropy}}

    GROUP BY 1
    HAVING count(*) > 1
  );

  -- Remove duplicates from the table
  DELETE FROM {{.scratch_schema}}.pv_page_view_events{{.entropy}} WHERE page_view_id IN (SELECT page_view_id FROM {{.scratch_schema}}.pv_page_view_id_duplicates_this_run{{.entropy}});

END;
