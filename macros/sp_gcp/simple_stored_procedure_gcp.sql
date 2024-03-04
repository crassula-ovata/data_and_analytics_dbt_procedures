{% macro simple_stored_procedure_gcp() %}

    var sql_command = snowflake.createStatement({ sqlText:"SELECT sjs.STEP_SQL FROM SQL_JOB sj JOIN SQL_JOB_STEP sjs on sj.JOB_ID = sjs.JOB_ID WHERE Job_Name ilike ''"+JOBNAME+"'' ORDER BY Job_Name,STEP_ORDER;" });

    try {

        var steps = sql_command.execute ();
        
        while (steps.next())
        {
            var dml_command = snowflake.createStatement({ sqlText: steps.getColumnValue(1)});
            dml_command.execute();
        }
            
        return JOBNAME + " Succeeded.";
        }
    catch (err)  {
        var err_command = snowflake.createStatement({ sqlText: "rollback;"});
        err_command.execute();
        return JOBNAME + " Failed: " + err;
        }

{% endmacro %}