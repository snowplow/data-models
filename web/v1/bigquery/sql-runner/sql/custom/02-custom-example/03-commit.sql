
-- Commit table procedure handles committing to prod, including table creation, and creation of new columns if 'automigrate' is set to TRUE

CALL {{.output_schema}}.commit_table('{{.scratch_schema}}',                   -- sourceDataset
                                     'channel_engagement_staged{{.entropy}}', -- sourceTable
                                     '{{.output_schema}}',                    -- targetDataset
                                     'channel_engagement{{.entropy}}',        -- targetTable
                                     'page_view_id',                          -- joinKey
                                     'start_tstamp',                          -- partitionKey
                                     TRUE);                                   -- automigrate

-- If we like, we can manually create and update our production table instead.
