BEGIN;
  -- Update manifest
  DELETE
    FROM {{.output_schema}}.users_manifest{{.entropy}}
    WHERE domain_userid IN (SELECT domain_userid FROM {{.scratch_schema}}.users_userids_this_run{{.entropy}});

  INSERT INTO {{.output_schema}}.users_manifest{{.entropy}} (SELECT * FROM {{.scratch_schema}}.users_userids_this_run{{.entropy}});

  -- Truncate input table just processed
  TRUNCATE {{.scratch_schema}}.sessions_userid_manifest_staged{{.entropy}};

END;
