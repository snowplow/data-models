-- Define stored procedure so we can do a safety check
CREATE OR REPLACE PROCEDURE drop_events_staged_if_empty()
AS $$
DECLARE
    rows_in_table int;
BEGIN
    SELECT INTO rows_in_table COUNT(*) FROM {{.scratch_schema}}.events_staged{{.entropy}};
    IF rows_in_table > 0 THEN
        RAISE EXCEPTION 'Ensure that all data has been processed from events_staged and it is empty.';
    ELSE
        DROP TABLE {{.scratch_schema}}.events_staged{{.entropy}};
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Call the procedure
CALL drop_events_staged_if_empty();

-- Drop the procedure.
DROP PROCEDURE drop_events_staged_if_empty();