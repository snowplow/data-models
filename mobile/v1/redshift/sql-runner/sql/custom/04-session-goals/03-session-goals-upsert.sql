-- 3. DELETE - INSERT to production (and optionally drop the temp table)

BEGIN; --it is safest to use a transaction, in case of failure.

  DELETE FROM {{.output_schema}}.session_goals{{.entropy}}
    WHERE session_id IN (SELECT session_id FROM {{.scratch_schema}}.session_goals_staged{{.entropy}});

  INSERT INTO {{.output_schema}}.session_goals{{.entropy}}
    (SELECT * FROM {{.scratch_schema}}.session_goals_staged{{.entropy}});

END;
