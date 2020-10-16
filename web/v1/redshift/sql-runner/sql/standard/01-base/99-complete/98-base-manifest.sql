BEGIN;
  DELETE
    FROM {{.output_schema}}.base_event_id_manifest{{.entropy}}
    WHERE event_id IN (SELECT event_id FROM {{.scratch_schema}}.base_event_ids_this_run{{.entropy}})
    AND collector_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.base_run_limits{{.entropy}});

  INSERT INTO {{.output_schema}}.base_event_id_manifest{{.entropy}} (SELECT * FROM {{.scratch_schema}}.base_event_ids_this_run{{.entropy}});

  -- Commit session_id manifest
  DELETE
    FROM {{.output_schema}}.base_session_id_manifest{{.entropy}}
    WHERE session_id IN (SELECT session_id FROM {{.scratch_schema}}.base_sessions_to_include{{.entropy}});

  INSERT INTO {{.output_schema}}.base_session_id_manifest{{.entropy}} (SELECT * FROM {{.scratch_schema}}.base_sessions_to_include{{.entropy}});
END;
