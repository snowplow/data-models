-- 3. DELETE - INSERT to production (and optionally drop the temp table)

BEGIN; --it is safest to use a transaction, in case of failure.

  DELETE FROM {{.output_schema}}.page_views_join{{.entropy}}
    WHERE page_view_id IN (SELECT page_view_id FROM {{.scratch_schema}}.page_views_join_staged{{.entropy}});

  INSERT INTO {{.output_schema}}.page_views_join{{.entropy}}
    (SELECT * FROM {{.scratch_schema}}.page_views_join_staged{{.entropy}});

END;
