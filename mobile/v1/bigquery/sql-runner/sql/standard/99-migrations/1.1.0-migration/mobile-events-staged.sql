-- Create Procedure to drop mobile_events_staged if it is empty
CREATE OR REPLACE PROCEDURE {{.scratch_schema}}.drop_mobile_events_staged_if_empty ()
BEGIN
    DECLARE rows_in_table INT64;
    SET rows_in_table = (SELECT count(*) FROM {{.scratch_schema}}.mobile_events_staged{{.entropy}});

    IF rows_in_table > 0 THEN
         RAISE USING MESSAGE = 'Ensure that all data has been processed from events_staged and it is empty.';
    ELSE
        DROP TABLE {{.scratch_schema}}.mobile_events_staged{{.entropy}};
    END IF;
END;

-- Run the procedure
CALL {{.scratch_schema}}.drop_mobile_events_staged_if_empty();

-- Remove the procedure
DROP PROCEDURE {{.scratch_schema}}.drop_mobile_events_staged_if_empty;
