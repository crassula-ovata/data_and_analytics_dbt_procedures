{% macro sp_data_unload() %}

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
      
      RAISE step_error_exception;
  ELSE 
      --start looping through steps
      FOR i IN 0 TO step_count DO
    
          --get current step
          SELECT upper(split(:steps, ''|'')[:i]) INTO :step;
    
          --case statement to run current step; this allows different combinations of steps
          CASE step
          WHEN ''UNLOAD_SF_TO_S3_AWS'' THEN
              CALL 
                {% if target.name=='dev' %}
                METADATA.PROCEDURES_DEV
                {% elif target.name=='qa' %}
                METADATA.PROCEDURES_QA
                {% elif target.name=='prod' %}
                METADATA.PROCEDURES
                {% elif target.name=='test' %}
                METADATA.PROCEDURES_TEST
                {% else %}
                invalid
                {% endif %}               
              .sp_s3_data_unload(''DATA_UNLOAD'', ''client'', ''DM.VW_CLIENT_DUPLICATES_API_PAYLOAD'', :domain, :dl_db, :dl_schema, :dm_db, :task_id, :path_time_override) INTO :step_result;
          WHEN ''UNLOAD_SF_TO_S3_GCP'' THEN
              CALL 
                {% if target.name=='dev' %}
                METADATA.PROCEDURES_DEV
                {% elif target.name=='qa' %}
                METADATA.PROCEDURES_QA
                {% elif target.name=='prod' %}
                METADATA.PROCEDURES
                {% elif target.name=='test' %}
                METADATA.PROCEDURES_TEST
                {% else %}
                invalid
                {% endif %}               
              .sp_s3_data_unload(''DATA_UNLOAD'', ''ladders'', ''DM.VW_LADDERS_API_PAYLOAD'', :domain, :dl_db, :dl_schema, :dm_db, :task_id, :path_time_override) INTO :step_result;
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