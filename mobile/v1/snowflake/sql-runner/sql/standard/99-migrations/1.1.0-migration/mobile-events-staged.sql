-- Create Procedure to drop mobile_events_staged if it is empty
CREATE OR REPLACE PROCEDURE {{.scratch_schema}}.drop_mobile_events_staged_if_empty()
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
  AS
  $$
    var getRowCount = snowflake.createStatement(
           {
           sqlText: "SELECT count(*) FROM {{.scratch_schema}}.mobile_events_staged{{.entropy}};"
           }
        );
	
    var res = getRowCount.execute();
    res.next();
    row_count = res.getColumnValue(1);
    if (row_count > 0) {
        throw "Ensure that all data has been processed from events_staged and it is empty.";
    }
    else {
        try {
            var getRowCount = snowflake.createStatement(
                {
                sqlText: "DROP TABLE {{.scratch_schema}}.mobile_events_staged{{.entropy}};"
                }
                );

            var res = getRowCount.execute();
        }  catch(ERROR) {

            snowflake.createStatement({sqlText: `ROLLBACK;`}).execute();
            throw ERROR;

        }
    }
  $$;

-- Run the procedure
CALL {{.scratch_schema}}.drop_mobile_events_staged_if_empty();


-- Remove the procedure
DROP PROCEDURE {{.scratch_schema}}.drop_mobile_events_staged_if_empty();