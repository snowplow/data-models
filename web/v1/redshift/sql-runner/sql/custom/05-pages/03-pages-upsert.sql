-- 3. DELETE - INSERT to production (and optionally drop the temp table)

BEGIN; --it is safest to use a transaction, in case of failure.

  DELETE FROM {{.output_schema}}.pages{{.entropy}}
    WHERE page_urlhost IN (SELECT page_urlhost FROM {{.scratch_schema}}.pages_staged{{.entropy}});

  INSERT INTO {{.output_schema}}.pages{{.entropy}}
    (SELECT * FROM {{.scratch_schema}}.pages_staged{{.entropy}});

END;
