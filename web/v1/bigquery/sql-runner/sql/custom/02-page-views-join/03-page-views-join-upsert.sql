-- 3. DELETE - INSERT to production (and optionally drop the temp table)


CALL {{.output_schema}}.commit_table('{{.scratch_schema}}',                   -- sourceDataset
                                     'page_views_join_staged{{.entropy}}',    -- sourceTable
                                     '{{.output_schema}}',                    -- targetDataset
                                     'page_views_join{{.entropy}}',           -- targetTable
                                     'page_view_id',                          -- joinKey
                                     'start_tstamp',                          -- partitionKey
                                     TRUE);                                   -- automigrate
