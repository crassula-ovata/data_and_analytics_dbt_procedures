{% macro sp_ingest_transform_old() %}

DECLARE
    task_id integer;
    uuid default uuid_string();
    start_dt default SYSDATE();
    step_exception EXCEPTION (-20001, ''Invalid step name: '');
    step_error_exception EXCEPTION (-20003, ''Error during execution of step: '');
    step_count integer default 0;
    step string;
    step_result string default null;
BEGIN
  --create task log record
  EXECUTE IMMEDIATE ''INSERT INTO '' || dm_db || ''.UTIL.TASK_LOG (UUID, TYPE, SUBTYPE, DOMAIN, STATUS, TASK_START) VALUES(\\'''' || uuid || ''\\'', \\'''' || type || ''\\'', \\'''' || subtype || ''\\'', \\'''' || domain || ''\\'', \\''START\\'', \\'''' || start_dt || ''\\''::timestamp_ntz);'';
  --get task log id
  LET ref string := :dm_db || ''.UTIL.TASK_LOG'';
  SELECT TASK_ID INTO :task_id FROM IDENTIFIER(:ref) WHERE UUID = :uuid and STATUS = ''START'';

  --get count of steps
  SELECT REGEXP_COUNT(:steps, ''\\\\|'')-1 INTO :step_count;

  IF (step_count < 0) THEN --run the steps param if no steps defined with delimiter
      step := steps;
      
      CALL METADATA.PROCEDURES.SP_RUN_JOB('''', :steps, :type, :subtype, :domain, :dl_db, :dl_schema, :dm_db, :task_id) INTO :step_result;

      --check result of step for error
      IF (step_result ilike ''%error%'') THEN 
          RAISE step_error_exception;
      END IF;
  ELSE 
      --start looping through steps
      FOR i IN 0 TO step_count DO
    
          --get current step
          SELECT upper(split(:steps, ''|'')[:i]) INTO :step;
    
          --case statement to run current step; this allows different combinations of steps
          CASE step
          WHEN ''S3_INGEST'' THEN
              CALL METADATA.PROCEDURES.sp_s3_ingest(''DATA_LOAD'', ''RAW_STAGE'', :domain, :dl_db, :dl_schema, :dm_db, :task_id, :path_time_override) INTO :step_result;
          WHEN ''STAGE_TO_RAW'' THEN
              CALL METADATA.PROCEDURES.SP_RUN_JOB(''Raw Data Load'', '''', ''DATA_LOAD'', ''STAGE_TO_RAW'', :domain, :dl_db, :dl_schema, :dm_db, :task_id) INTO :step_result; 
          WHEN ''UPDATE_CONFIG'' THEN
              CALL METADATA.PROCEDURES.SP_RUN_JOB(''Update Config'', '''', ''CONFIG'', ''FIELD_CONFIG'', :domain, :dl_db, :dl_schema, :dm_db, :task_id) INTO :step_result; 
          WHEN ''RECREATE_VIEWS'' THEN
              CALL METADATA.PROCEDURES.SP_RUN_JOB('''', ''SELECT SQL_TEXT FROM '' || :dm_db || ''.INTEGRATION.GENERATE_ALL_VIEWS;'', ''TRANSFORM'', ''RECREATE_VIEWS'', :domain, :dl_db, :dl_schema, :dm_db, :task_id) INTO :step_result; 
          WHEN ''RECREATE_TABLES'' THEN
              CALL METADATA.PROCEDURES.SP_RUN_JOB('''', ''SELECT SQL_TEXT FROM '' || :dm_db || ''.INTEGRATION.GENERATE_ALL_TABLES;'', ''TRANSFORM'', ''RECREATE_TABLES'', :domain, :dl_db, :dl_schema, :dm_db, :task_id) INTO :step_result; 
          WHEN ''INCR_TABLES'' THEN
              CALL METADATA.PROCEDURES.SP_RUN_JOB('''', ''SELECT SQL_TEXT FROM '' || :dm_db || ''.INTEGRATION.generate_all_tables_incr_load;'', ''TRANSFORM'', ''TABLES_INCR'', :domain, :dl_db, :dl_schema, :dm_db, :task_id) INTO :step_result; 
          WHEN ''STAGE_DELETE'' THEN
              CALL METADATA.PROCEDURES.SP_RUN_JOB(''Raw Data Stage Delete'', '''', ''DATA_LOAD'', ''STAGE_DELETE'', :domain, :dl_db, :dl_schema, :dm_db, :task_id) INTO :step_result;
          ELSE
              RAISE step_exception;
          END CASE;

          --check result of step for error
          IF (step_result ilike ''%error%'') THEN 
              RAISE step_error_exception;
          END IF;
      END FOR;
  END IF;
  
  -- log status
  EXECUTE IMMEDIATE ''UPDATE '' || dm_db || ''.UTIL.TASK_LOG SET STATUS = \\''SUCCESS\\'', TASK_END = SYSDATE() WHERE TASK_ID = '' || task_id || '';'';
  
  RETURN ''Finished calling ingest/transform stored procedures, task id: '' || task_id;
  
EXCEPTION
  WHEN statement_error THEN 
      EXECUTE IMMEDIATE ''INSERT INTO '' || dm_db || ''.UTIL.MESSAGE_LOG (TASK_ID, TYPE, SUBTYPE, MESSAGE) SELECT '' || task_id || '', \\''ERROR\\'', \\''STATEMENT_ERROR\\'', OBJECT_CONSTRUCT(\\''ERROR_TYPE\\'', \\''STATEMENT_ERROR\\'', \\''SQLCODE\\'', \\'''' || sqlcode || ''\\'', \\''SQLERRM\\'', \\'''' || replace(sqlerrm, ''\\'''', ''\\\\\\'''') || ''\\'', \\''SQLSTATE\\'', \\'''' || sqlstate || ''\\'');'';
      EXECUTE IMMEDIATE ''UPDATE '' || dm_db || ''.UTIL.TASK_LOG SET STATUS = \\''FAILURE\\'', TASK_END = SYSDATE() WHERE TASK_ID = '' || task_id || '';'';
      RETURN ''Error calling ingest/transform stored procedures, task id: '' || task_id;
  WHEN expression_error THEN
      EXECUTE IMMEDIATE ''INSERT INTO '' || dm_db || ''.UTIL.MESSAGE_LOG (TASK_ID, TYPE, SUBTYPE, MESSAGE) SELECT '' || task_id || '', \\''ERROR\\'', \\''EXPRESSION_ERROR\\'', OBJECT_CONSTRUCT(\\''ERROR_TYPE\\'', \\''EXPRESSION_ERROR\\'', \\''SQLCODE\\'', \\'''' || sqlcode || ''\\'', \\''SQLERRM\\'', \\'''' || replace(sqlerrm, ''\\'''', ''\\\\\\'''') || ''\\'', \\''SQLSTATE\\'', \\'''' || sqlstate || ''\\'');'';
      EXECUTE IMMEDIATE ''UPDATE '' || dm_db || ''.UTIL.TASK_LOG SET STATUS = \\''FAILURE\\'', TASK_END = SYSDATE() WHERE TASK_ID = '' || task_id || '';'';
      RETURN ''Error calling ingest/transform stored procedures, task id: '' || task_id;
  WHEN step_exception THEN
      EXECUTE IMMEDIATE ''INSERT INTO '' || dm_db || ''.UTIL.MESSAGE_LOG (TASK_ID, TYPE, SUBTYPE, MESSAGE) SELECT '' || task_id || '', \\''ERROR\\'', \\''STEP_EXCEPTION\\'', OBJECT_CONSTRUCT(\\''ERROR_TYPE\\'', \\''STEP_EXCEPTION\\'', \\''SQLCODE\\'', \\'''' || sqlcode || ''\\'', \\''SQLERRM\\'', \\'''' || replace(sqlerrm, ''\\'''', ''\\\\\\'''') || step || ''\\'', \\''SQLSTATE\\'', \\'''' || sqlstate || ''\\'');'';
      EXECUTE IMMEDIATE ''UPDATE '' || dm_db || ''.UTIL.TASK_LOG SET STATUS = \\''FAILURE\\'', TASK_END = SYSDATE() WHERE TASK_ID = '' || task_id || '';'';
      RETURN ''Error calling ingest/transform stored procedures, task id: '' || task_id;
  WHEN step_error_exception THEN
      EXECUTE IMMEDIATE ''INSERT INTO '' || dm_db || ''.UTIL.MESSAGE_LOG (TASK_ID, TYPE, SUBTYPE, MESSAGE) SELECT '' || task_id || '', \\''ERROR\\'', \\''STEP_EXCEPTION\\'', OBJECT_CONSTRUCT(\\''ERROR_TYPE\\'', \\''STEP_ERROR_EXCEPTION\\'', \\''SQLCODE\\'', \\'''' || sqlcode || ''\\'', \\''SQLERRM\\'', \\'''' || replace(sqlerrm, ''\\'''', ''\\\\\\'''') || step || ''\\'', \\''SQLSTATE\\'', \\'''' || sqlstate || ''\\'');'';
      EXECUTE IMMEDIATE ''UPDATE '' || dm_db || ''.UTIL.TASK_LOG SET STATUS = \\''FAILURE\\'', TASK_END = SYSDATE() WHERE TASK_ID = '' || task_id || '';'';
      RETURN ''Error calling ingest/transform stored procedures, task id: '' || task_id;
  WHEN OTHER THEN
      EXECUTE IMMEDIATE ''INSERT INTO '' || dm_db || ''.UTIL.MESSAGE_LOG (TASK_ID, TYPE, SUBTYPE, MESSAGE) SELECT '' || task_id || '', \\''ERROR\\'', \\''OTHER_ERROR\\'', OBJECT_CONSTRUCT(\\''ERROR_TYPE\\'', \\''OTHER_ERROR\\'', \\''SQLCODE\\'', \\'''' || sqlcode || ''\\'', \\''SQLERRM\\'', \\'''' || replace(sqlerrm, ''\\'''', ''\\\\\\'''') || ''\\'', \\''SQLSTATE\\'', \\'''' || sqlstate || ''\\'');'';
      EXECUTE IMMEDIATE ''UPDATE '' || dm_db || ''.UTIL.TASK_LOG SET STATUS = \\''FAILURE\\'', TASK_END = SYSDATE() WHERE TASK_ID = '' || task_id || '';'';
      RETURN ''Error calling ingest/transform stored procedures, task id: '' || task_id;
END

{% endmacro %}