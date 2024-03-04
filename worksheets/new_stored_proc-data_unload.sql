use role sysadmin;
use database DATALAKE_PROD;
use schema CO_CARE_COORDINATION_COMMCARE_PROD;

CREATE OR REPLACE file format sf_to_s3_unload_file_format
	type=JSON TIME_FORMAT=AUTO DATE_FORMAT=AUTO COMPRESSION=NONE FILE_EXTENSION='json';
    
CREATE OR REPLACE STAGE s3_json_payload_stage
    url='s3://commcare-snowflake-data-sync/co-carecoordination/payload/'
    Storage_integration = s3_int_obj
    file_format = sf_to_s3_unload_file_format;

-- create task
/*
create or replace task DATALAKE_PROD.CO_CARE_COORDINATION_COMMCARE_PROD.S3_UNLOAD_TASK
    warehouse=COMPUTE_WH
	--schedule='USING CRON 00 22 * * * America/New_York'
	schedule='USING CRON 15 05 * * * America/New_York'
	error_integration=SNS_INT_OBJ
	as DECLARE 
    task_result string default null;
    task_exception EXCEPTION (-20003, 'Task had an error');
BEGIN
    Call metadata.procedures.sp_data_unload('CLIENT_PAYLOAD_UPDATE', 'task_call_sp_data_unload', 'co-carecoordination', 'DATALAKE_PROD', 'CO_CARE_COORDINATION_COMMCARE_PROD', 'DM_CO_CARE_COORD_PROD', 'UNLOAD_SF_TO_S3_AWS|', null) into :task_result;
    IF (task_result ilike '%error%') THEN 
        RAISE task_exception;
    END IF;
END;

ALTER TASK DATALAKE_PROD.CO_CARE_COORDINATION_COMMCARE_PROD.S3_UNLOAD_TASK RESUME;
EXECUTE TASK DATALAKE_PROD.CO_CARE_COORDINATION_COMMCARE_PROD.S3_UNLOAD_TASK;
*/
-- end create task

    
use role sysadmin;
use database METADATA;
use schema procedures;

CREATE OR REPLACE PROCEDURE sp_data_unload(type STRING, subtype STRING, domain STRING, dl_db STRING, dl_schema STRING, dm_db STRING, steps STRING, path_time_override STRING)
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE
    task_id integer;
    uuid default uuid_string();
    start_dt default SYSDATE();
    step_exception EXCEPTION (-20001, 'Invalid step name: ');
    step_error_exception EXCEPTION (-20003, 'Error during execution of step: ');
    step_count integer default 0;
    step string;
    step_result string default null;
BEGIN
  --create task log record
  EXECUTE IMMEDIATE 'INSERT INTO ' || dm_db || '.UTIL.TASK_LOG (UUID, TYPE, SUBTYPE, DOMAIN, STATUS, TASK_START) VALUES(\'' || uuid || '\', \'' || type || '\', \'' || subtype || '\', \'' || domain || '\', \'START\', \'' || start_dt || '\'::timestamp_ntz);';
  --get task log id
  LET ref string := :dm_db || '.UTIL.TASK_LOG';
  SELECT TASK_ID INTO :task_id FROM IDENTIFIER(:ref) WHERE UUID = :uuid and STATUS = 'START';

  --get count of steps
  SELECT REGEXP_COUNT(:steps, '\\|')-1 INTO :step_count;

  IF (step_count < 0) THEN --run the steps param if no steps defined with delimiter
      step := steps;
      
      RAISE step_error_exception;
  ELSE 
      --start looping through steps
      FOR i IN 0 TO step_count DO
    
          --get current step
          SELECT upper(split(:steps, '|')[:i]) INTO :step;
    
          --case statement to run current step; this allows different combinations of steps
          CASE step
          WHEN 'UNLOAD_SF_TO_S3_AWS' THEN
              CALL METADATA.PROCEDURES.sp_s3_data_unload('DATA_UNLOAD', 'client', 'DM.VW_CLIENT_DUPLICATES_API_PAYLOAD', :domain, :dl_db, :dl_schema, :dm_db, :task_id, :path_time_override) INTO :step_result;
          WHEN 'UNLOAD_SF_TO_S3_GCP' THEN
              CALL METADATA.PROCEDURES.sp_s3_data_unload('DATA_UNLOAD', 'ladders', 'DM.VW_LADDERS_API_PAYLOAD', :domain, :dl_db, :dl_schema, :dm_db, :task_id, :path_time_override) INTO :step_result;
          ELSE
              RAISE step_exception;
          END CASE;

          --check result of step for error
          IF (step_result ilike '%error%') THEN 
              RAISE step_error_exception;
          END IF;
      END FOR;
  END IF;
  
  -- log status
  EXECUTE IMMEDIATE 'UPDATE ' || dm_db || '.UTIL.TASK_LOG SET STATUS = \'SUCCESS\', TASK_END = SYSDATE() WHERE TASK_ID = ' || task_id || ';';
  
  RETURN 'Finished calling ingest/transform stored procedures, task id: ' || task_id;
  
EXCEPTION
  WHEN statement_error THEN 
      EXECUTE IMMEDIATE 'INSERT INTO ' || dm_db || '.UTIL.MESSAGE_LOG (TASK_ID, TYPE, SUBTYPE, MESSAGE) SELECT ' || task_id || ', \'ERROR\', \'STATEMENT_ERROR\', OBJECT_CONSTRUCT(\'ERROR_TYPE\', \'STATEMENT_ERROR\', \'SQLCODE\', \'' || sqlcode || '\', \'SQLERRM\', \'' || replace(sqlerrm, '\'', '\\\'') || '\', \'SQLSTATE\', \'' || sqlstate || '\');';
      EXECUTE IMMEDIATE 'UPDATE ' || dm_db || '.UTIL.TASK_LOG SET STATUS = \'FAILURE\', TASK_END = SYSDATE() WHERE TASK_ID = ' || task_id || ';';
      RETURN 'Error calling ingest/transform stored procedures, task id: ' || task_id;
  WHEN expression_error THEN
      EXECUTE IMMEDIATE 'INSERT INTO ' || dm_db || '.UTIL.MESSAGE_LOG (TASK_ID, TYPE, SUBTYPE, MESSAGE) SELECT ' || task_id || ', \'ERROR\', \'EXPRESSION_ERROR\', OBJECT_CONSTRUCT(\'ERROR_TYPE\', \'EXPRESSION_ERROR\', \'SQLCODE\', \'' || sqlcode || '\', \'SQLERRM\', \'' || replace(sqlerrm, '\'', '\\\'') || '\', \'SQLSTATE\', \'' || sqlstate || '\');';
      EXECUTE IMMEDIATE 'UPDATE ' || dm_db || '.UTIL.TASK_LOG SET STATUS = \'FAILURE\', TASK_END = SYSDATE() WHERE TASK_ID = ' || task_id || ';';
      RETURN 'Error calling ingest/transform stored procedures, task id: ' || task_id;
  WHEN step_exception THEN
      EXECUTE IMMEDIATE 'INSERT INTO ' || dm_db || '.UTIL.MESSAGE_LOG (TASK_ID, TYPE, SUBTYPE, MESSAGE) SELECT ' || task_id || ', \'ERROR\', \'STEP_EXCEPTION\', OBJECT_CONSTRUCT(\'ERROR_TYPE\', \'STEP_EXCEPTION\', \'SQLCODE\', \'' || sqlcode || '\', \'SQLERRM\', \'' || replace(sqlerrm, '\'', '\\\'') || step || '\', \'SQLSTATE\', \'' || sqlstate || '\');';
      EXECUTE IMMEDIATE 'UPDATE ' || dm_db || '.UTIL.TASK_LOG SET STATUS = \'FAILURE\', TASK_END = SYSDATE() WHERE TASK_ID = ' || task_id || ';';
      RETURN 'Error calling ingest/transform stored procedures, task id: ' || task_id;
  WHEN step_error_exception THEN
      EXECUTE IMMEDIATE 'INSERT INTO ' || dm_db || '.UTIL.MESSAGE_LOG (TASK_ID, TYPE, SUBTYPE, MESSAGE) SELECT ' || task_id || ', \'ERROR\', \'STEP_EXCEPTION\', OBJECT_CONSTRUCT(\'ERROR_TYPE\', \'STEP_ERROR_EXCEPTION\', \'SQLCODE\', \'' || sqlcode || '\', \'SQLERRM\', \'' || replace(sqlerrm, '\'', '\\\'') || step || '\', \'SQLSTATE\', \'' || sqlstate || '\');';
      EXECUTE IMMEDIATE 'UPDATE ' || dm_db || '.UTIL.TASK_LOG SET STATUS = \'FAILURE\', TASK_END = SYSDATE() WHERE TASK_ID = ' || task_id || ';';
      RETURN 'Error calling ingest/transform stored procedures, task id: ' || task_id;
  WHEN OTHER THEN
      EXECUTE IMMEDIATE 'INSERT INTO ' || dm_db || '.UTIL.MESSAGE_LOG (TASK_ID, TYPE, SUBTYPE, MESSAGE) SELECT ' || task_id || ', \'ERROR\', \'OTHER_ERROR\', OBJECT_CONSTRUCT(\'ERROR_TYPE\', \'OTHER_ERROR\', \'SQLCODE\', \'' || sqlcode || '\', \'SQLERRM\', \'' || replace(sqlerrm, '\'', '\\\'') || '\', \'SQLSTATE\', \'' || sqlstate || '\');';
      EXECUTE IMMEDIATE 'UPDATE ' || dm_db || '.UTIL.TASK_LOG SET STATUS = \'FAILURE\', TASK_END = SYSDATE() WHERE TASK_ID = ' || task_id || ';';
      RETURN 'Error calling ingest/transform stored procedures, task id: ' || task_id;
END
;



CREATE OR REPLACE PROCEDURE sp_s3_data_unload(type STRING, subtype STRING, payload_name STRING, domain STRING, dl_db STRING, dl_schema STRING, dm_db STRING, task_id INTEGER, path_time_override STRING)
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE
    exec_id integer;
    uuid default uuid_string();
    start_dt default SYSDATE();
    s3_path_root string;
    s3_path_time string default coalesce(path_time_override, to_char(sysdate(), '/YYYY/MM/DD/HH/'));
    s3_path string;
    recs resultset;
BEGIN
    --create exec log record
    EXECUTE IMMEDIATE 'INSERT INTO ' || dm_db || '.UTIL.EXECUTION_LOG (TASK_ID, UUID, TYPE, SUBTYPE, DOMAIN, STATUS, EXECUTION_START) VALUES(' || task_id || ', \'' || uuid || '\', \'' || type || '\', \'' || subtype || '\', \'' || domain || '\', \'START\', \'' || start_dt || '\'::timestamp_ntz);';
    --get exec log id
    LET ref string := :dm_db || '.UTIL.EXECUTION_LOG';
    SELECT EXECUTION_ID INTO :exec_id FROM IDENTIFIER(:ref) WHERE UUID = :uuid and STATUS = 'START';

    --get root s3 path for current project
    ref := '@' || :dl_db || '.' || :dl_schema || '.s3_json_payload_stage';
    SELECT GET_STAGE_LOCATION(:ref) INTO :s3_path_root;
     
    --set s3 stage
    s3_path := :s3_path_root || :subtype || :s3_path_time;     
    EXECUTE IMMEDIATE 
        $$CREATE OR REPLACE STAGE $$ || dl_db || $$.$$ || dl_schema || $$.s3_json_dump_case
            url='$$ || s3_path || $$'
            Storage_integration = s3_int_obj
            file_format = $$ || dl_db || $$.$$ || dl_schema || $$.sf_to_s3_unload_file_format;
        $$;

    --get records to unload
    recs := (EXECUTE IMMEDIATE $$SELECT grouping::string grp, payload from $$ || dm_db || $$.$$ || payload_name || $$ order by grouping;$$);

    --set a cursor to loop through recs
    LET c cursor FOR recs;
    OPEN c;

    --loop through recs
    FOR rec IN c DO
        --output one file per record
        EXECUTE IMMEDIATE             
            $$
                COPY INTO @s3_json_dump_case/case_data_$$ || rec.grp || $$.json 
                from (SELECT parse_json('$$ || replace(rec.payload, '\'', '\\\'') || $$'))
                    file_format = $$ || dl_db || $$.$$ || dl_schema || $$.sf_to_s3_unload_file_format
                    SINGLE=TRUE
                    OVERWRITE=TRUE;
            $$; 
    END FOR;

    --Log Success
    EXECUTE IMMEDIATE 'UPDATE ' || dm_db || '.UTIL.EXECUTION_LOG SET STATUS = \'SUCCESS\', EXECUTION_END = SYSDATE() WHERE EXECUTION_ID = ' || exec_id || ';';
    EXECUTE IMMEDIATE 
    $$insert into $$ || dm_db || $$.util.sql_logs (TASK_ID,EXECUTION_ID,QUERY_ID,QUERY_TEXT,DATABASE_NAME,SCHEMA_NAME,QUERY_TYPE,SESSION_ID,USER_NAME,ROLE_NAME,WAREHOUSE_NAME,WAREHOUSE_SIZE,WAREHOUSE_TYPE,CLUSTER_NUMBER,QUERY_TAG,EXECUTION_STATUS,ERROR_CODE,ERROR_MESSAGE,START_TIME,END_TIME,TOTAL_ELAPSED_TIME,BYTES_SCANNED,ROWS_PRODUCED,COMPILATION_TIME,EXECUTION_TIME,QUEUED_PROVISIONING_TIME,QUEUED_REPAIR_TIME,QUEUED_OVERLOAD_TIME,TRANSACTION_BLOCKED_TIME,OUTBOUND_DATA_TRANSFER_CLOUD,OUTBOUND_DATA_TRANSFER_REGION,OUTBOUND_DATA_TRANSFER_BYTES,INBOUND_DATA_TRANSFER_CLOUD,INBOUND_DATA_TRANSFER_REGION,INBOUND_DATA_TRANSFER_BYTES,CREDITS_USED_CLOUD_SERVICES,LIST_EXTERNAL_FILE_TIME,RELEASE_VERSION,EXTERNAL_FUNCTION_TOTAL_INVOCATIONS,EXTERNAL_FUNCTION_TOTAL_SENT_ROWS,EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS,EXTERNAL_FUNCTION_TOTAL_SENT_BYTES,EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES,IS_CLIENT_GENERATED_STATEMENT,QUERY_HASH,QUERY_HASH_VERSION,QUERY_PARAMETERIZED_HASH,QUERY_PARAMETERIZED_HASH_VERSION)
    select $$ || task_id || $$ TASK_ID, $$ || exec_id || $$ EXECUTION_ID, 
    * from table(information_schema.query_history_by_session(RESULT_LIMIT => 10000)) where execution_status<>'RUNNING' and convert_timezone('UTC', START_TIME)::timestamp_ntz >= '$$ || start_dt || $$'::timestamp_ntz
    order by start_time
    ;$$;
    RETURN 'Finished executing procedure, exec id: ' || exec_id;
    
EXCEPTION
  WHEN statement_error THEN 
      EXECUTE IMMEDIATE 'INSERT INTO ' || dm_db || '.UTIL.MESSAGE_LOG (TASK_ID, EXECUTION_ID, TYPE, SUBTYPE, MESSAGE) SELECT ' || task_id || ', ' || exec_id || ', \'ERROR\', \'STATEMENT_ERROR\', OBJECT_CONSTRUCT(\'ERROR_TYPE\', \'STATEMENT_ERROR\', \'SQLCODE\', \'' || sqlcode || '\', \'SQLERRM\', \'' || replace(sqlerrm, '\'', '\\\'') || '\', \'SQLSTATE\', \'' || sqlstate || '\');';
      EXECUTE IMMEDIATE 'UPDATE ' || dm_db || '.UTIL.EXECUTION_LOG SET STATUS = \'FAILURE\', EXECUTION_END = SYSDATE() WHERE EXECUTION_ID = ' || exec_id || ';';
      EXECUTE IMMEDIATE 
      $$insert into $$ || dm_db || $$.util.sql_logs (TASK_ID,EXECUTION_ID,QUERY_ID,QUERY_TEXT,DATABASE_NAME,SCHEMA_NAME,QUERY_TYPE,SESSION_ID,USER_NAME,ROLE_NAME,WAREHOUSE_NAME,WAREHOUSE_SIZE,WAREHOUSE_TYPE,CLUSTER_NUMBER,QUERY_TAG,EXECUTION_STATUS,ERROR_CODE,ERROR_MESSAGE,START_TIME,END_TIME,TOTAL_ELAPSED_TIME,BYTES_SCANNED,ROWS_PRODUCED,COMPILATION_TIME,EXECUTION_TIME,QUEUED_PROVISIONING_TIME,QUEUED_REPAIR_TIME,QUEUED_OVERLOAD_TIME,TRANSACTION_BLOCKED_TIME,OUTBOUND_DATA_TRANSFER_CLOUD,OUTBOUND_DATA_TRANSFER_REGION,OUTBOUND_DATA_TRANSFER_BYTES,INBOUND_DATA_TRANSFER_CLOUD,INBOUND_DATA_TRANSFER_REGION,INBOUND_DATA_TRANSFER_BYTES,CREDITS_USED_CLOUD_SERVICES,LIST_EXTERNAL_FILE_TIME,RELEASE_VERSION,EXTERNAL_FUNCTION_TOTAL_INVOCATIONS,EXTERNAL_FUNCTION_TOTAL_SENT_ROWS,EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS,EXTERNAL_FUNCTION_TOTAL_SENT_BYTES,EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES,IS_CLIENT_GENERATED_STATEMENT,QUERY_HASH,QUERY_HASH_VERSION,QUERY_PARAMETERIZED_HASH,QUERY_PARAMETERIZED_HASH_VERSION)
    select $$ || task_id || $$ TASK_ID, $$ || exec_id || $$ EXECUTION_ID, 
    * from table(information_schema.query_history_by_session(RESULT_LIMIT => 10000)) where execution_status<>'RUNNING' and convert_timezone('UTC', START_TIME)::timestamp_ntz >= '$$ || start_dt || $$'::timestamp_ntz
    order by start_time
    ;$$;
      RETURN 'Error executing procedure, exec id: ' || exec_id;
  WHEN expression_error THEN
      EXECUTE IMMEDIATE 'INSERT INTO ' || dm_db || '.UTIL.MESSAGE_LOG (TASK_ID, EXECUTION_ID, TYPE, SUBTYPE, MESSAGE) SELECT ' || task_id || ', ' || exec_id || ', \'ERROR\', \'EXPRESSION_ERROR\', OBJECT_CONSTRUCT(\'ERROR_TYPE\', \'EXPRESSION_ERROR\', \'SQLCODE\', \'' || sqlcode || '\', \'SQLERRM\', \'' || replace(sqlerrm, '\'', '\\\'') || '\', \'SQLSTATE\', \'' || sqlstate || '\');';
      EXECUTE IMMEDIATE 'UPDATE ' || dm_db || '.UTIL.EXECUTION_LOG SET STATUS = \'FAILURE\', EXECUTION_END = SYSDATE() WHERE EXECUTION_ID = ' || exec_id || ';';
      EXECUTE IMMEDIATE 
      $$insert into $$ || dm_db || $$.util.sql_logs (TASK_ID,EXECUTION_ID,QUERY_ID,QUERY_TEXT,DATABASE_NAME,SCHEMA_NAME,QUERY_TYPE,SESSION_ID,USER_NAME,ROLE_NAME,WAREHOUSE_NAME,WAREHOUSE_SIZE,WAREHOUSE_TYPE,CLUSTER_NUMBER,QUERY_TAG,EXECUTION_STATUS,ERROR_CODE,ERROR_MESSAGE,START_TIME,END_TIME,TOTAL_ELAPSED_TIME,BYTES_SCANNED,ROWS_PRODUCED,COMPILATION_TIME,EXECUTION_TIME,QUEUED_PROVISIONING_TIME,QUEUED_REPAIR_TIME,QUEUED_OVERLOAD_TIME,TRANSACTION_BLOCKED_TIME,OUTBOUND_DATA_TRANSFER_CLOUD,OUTBOUND_DATA_TRANSFER_REGION,OUTBOUND_DATA_TRANSFER_BYTES,INBOUND_DATA_TRANSFER_CLOUD,INBOUND_DATA_TRANSFER_REGION,INBOUND_DATA_TRANSFER_BYTES,CREDITS_USED_CLOUD_SERVICES,LIST_EXTERNAL_FILE_TIME,RELEASE_VERSION,EXTERNAL_FUNCTION_TOTAL_INVOCATIONS,EXTERNAL_FUNCTION_TOTAL_SENT_ROWS,EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS,EXTERNAL_FUNCTION_TOTAL_SENT_BYTES,EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES,IS_CLIENT_GENERATED_STATEMENT,QUERY_HASH,QUERY_HASH_VERSION,QUERY_PARAMETERIZED_HASH,QUERY_PARAMETERIZED_HASH_VERSION)
    select $$ || task_id || $$ TASK_ID, $$ || exec_id || $$ EXECUTION_ID, 
    * from table(information_schema.query_history_by_session(RESULT_LIMIT => 10000)) where execution_status<>'RUNNING' and convert_timezone('UTC', START_TIME)::timestamp_ntz >= '$$ || start_dt || $$'::timestamp_ntz
    order by start_time
    ;$$;
      RETURN 'Error executing procedures, exec id: ' || exec_id;
  WHEN OTHER THEN
      EXECUTE IMMEDIATE 'INSERT INTO ' || dm_db || '.UTIL.MESSAGE_LOG (TASK_ID, EXECUTION_ID, TYPE, SUBTYPE, MESSAGE) SELECT ' || task_id || ', ' || exec_id || ', \'ERROR\', \'OTHER_ERROR\', OBJECT_CONSTRUCT(\'ERROR_TYPE\', \'OTHER_ERROR\', \'SQLCODE\', \'' || sqlcode || '\', \'SQLERRM\', \'' || replace(sqlerrm, '\'', '\\\'') || '\', \'SQLSTATE\', \'' || sqlstate || '\');';
      EXECUTE IMMEDIATE 'UPDATE ' || dm_db || '.UTIL.EXECUTION_LOG SET STATUS = \'FAILURE\', EXECUTION_END = SYSDATE() WHERE EXECUTION_ID = ' || exec_id || ';';
      EXECUTE IMMEDIATE 
      $$insert into $$ || dm_db || $$.util.sql_logs (TASK_ID,EXECUTION_ID,QUERY_ID,QUERY_TEXT,DATABASE_NAME,SCHEMA_NAME,QUERY_TYPE,SESSION_ID,USER_NAME,ROLE_NAME,WAREHOUSE_NAME,WAREHOUSE_SIZE,WAREHOUSE_TYPE,CLUSTER_NUMBER,QUERY_TAG,EXECUTION_STATUS,ERROR_CODE,ERROR_MESSAGE,START_TIME,END_TIME,TOTAL_ELAPSED_TIME,BYTES_SCANNED,ROWS_PRODUCED,COMPILATION_TIME,EXECUTION_TIME,QUEUED_PROVISIONING_TIME,QUEUED_REPAIR_TIME,QUEUED_OVERLOAD_TIME,TRANSACTION_BLOCKED_TIME,OUTBOUND_DATA_TRANSFER_CLOUD,OUTBOUND_DATA_TRANSFER_REGION,OUTBOUND_DATA_TRANSFER_BYTES,INBOUND_DATA_TRANSFER_CLOUD,INBOUND_DATA_TRANSFER_REGION,INBOUND_DATA_TRANSFER_BYTES,CREDITS_USED_CLOUD_SERVICES,LIST_EXTERNAL_FILE_TIME,RELEASE_VERSION,EXTERNAL_FUNCTION_TOTAL_INVOCATIONS,EXTERNAL_FUNCTION_TOTAL_SENT_ROWS,EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS,EXTERNAL_FUNCTION_TOTAL_SENT_BYTES,EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES,IS_CLIENT_GENERATED_STATEMENT,QUERY_HASH,QUERY_HASH_VERSION,QUERY_PARAMETERIZED_HASH,QUERY_PARAMETERIZED_HASH_VERSION)
    select $$ || task_id || $$ TASK_ID, $$ || exec_id || $$ EXECUTION_ID, 
    * from table(information_schema.query_history_by_session(RESULT_LIMIT => 10000)) where execution_status<>'RUNNING' and convert_timezone('UTC', START_TIME)::timestamp_ntz >= '$$ || start_dt || $$'::timestamp_ntz
    order by start_time
    ;$$;
      RETURN 'Error executing procedures, exec id: ' || exec_id;
END;
