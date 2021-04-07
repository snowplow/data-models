
-- 2. Commit table procedure handles committing to prod, including table creation, and creation of new columns if 'automigrate' is set to TRUE

CALL {{.output_schema}}.commit_table('{{.scratch_schema}}',                   -- sourceDataset
                                     'session_goals_staged{{.entropy}}',      -- sourceTable
                                     '{{.output_schema}}',                    -- targetDataset
                                     'session_goals{{.entropy}}',             -- targetTable
                                     'session_id',                            -- joinKey
                                     'start_tstamp',                          -- partitionKey
                                     TRUE);                                   -- automigrate

-- If we like, we can manually create and update our production table instead.
