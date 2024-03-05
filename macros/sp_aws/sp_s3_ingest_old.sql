{% macro sp_s3_ingest_old() %}

DECLARE
    exec_id integer;
    uuid default uuid_string();
    start_dt default SYSDATE();
    s3_path_root string;
    s3_path_time string default coalesce(path_time_override, to_char(dateadd(''HH'', -1, sysdate()), ''/YYYY/MM/DD/HH/''));
    s3_path string;
BEGIN
    --create exec log record
    EXECUTE IMMEDIATE ''INSERT INTO '' || dm_db || ''.UTIL.EXECUTION_LOG (TASK_ID, UUID, TYPE, SUBTYPE, DOMAIN, STATUS, EXECUTION_START) VALUES('' || task_id || '', \\'''' || uuid || ''\\'', \\'''' || type || ''\\'', \\'''' || subtype || ''\\'', \\'''' || domain || ''\\'', \\''START\\'', \\'''' || start_dt || ''\\''::timestamp_ntz);'';
    --get exec log id
    LET ref string := :dm_db || ''.UTIL.EXECUTION_LOG'';
    SELECT EXECUTION_ID INTO :exec_id FROM IDENTIFIER(:ref) WHERE UUID = :uuid and STATUS = ''START'';

    --get root s3 path for current project
    ref := ''@'' || :dl_db || ''.'' || :dl_schema || ''.s3_json_stage'';
    SELECT GET_STAGE_LOCATION(:ref) INTO :s3_path_root;

    --do cases
    EXECUTE IMMEDIATE ''TRUNCATE TABLE '' || dl_db || ''.'' || dl_schema || ''.CASES_RAW_STAGE;'';
    s3_path := :s3_path_root || ''case'' || :s3_path_time;
    EXECUTE IMMEDIATE 
        $$CREATE OR REPLACE STAGE $$ || dl_db || $$.$$ || dl_schema || $$.s3_json_stage_daily_case
            url=''$$ || s3_path || $$''
            Storage_integration = s3_int_obj
            file_format = $$ || dl_db || $$.$$ || dl_schema || $$.json_file_format;
    $$;
    EXECUTE IMMEDIATE 
        $$insert into $$ || dl_db || $$.$$ || dl_schema || $$.CASES_RAW_STAGE(domain, metadata, metadata_filename, json, id, SYSTEM_QUERY_TS, task_id, execution_id)
        select
         f.value:domain::string domain,
         s.$1:meta metadata,
         metadata$filename metadata_filename,
         f.value JSON,
         f.value:id::string id,
         replace(split_part(metadata_filename, ''_'', -1), ''.json'') SYSTEM_QUERY_TS,
         $$ || :task_id || $$ task_id,
         $$ || :exec_id || $$ execution_id
        from @$$ || dl_db || $$.$$ || dl_schema || $$.s3_json_stage_daily_case s, lateral flatten(s.$1:objects) f
        qualify row_number() over (partition by id order by metadata$file_last_modified desc) = 1
        ;$$;
        
    --do forms
    EXECUTE IMMEDIATE ''TRUNCATE TABLE '' || dl_db || ''.'' || dl_schema || ''.FORMS_RAW_STAGE;'';
    s3_path := s3_path_root || ''form'' || s3_path_time;
    EXECUTE IMMEDIATE 
        $$CREATE OR REPLACE STAGE $$ || dl_db || $$.$$ || dl_schema || $$.s3_json_stage_daily_form
            url=''$$ || s3_path || $$''
            Storage_integration = s3_int_obj
            file_format = $$ || dl_db || $$.$$ || dl_schema || $$.json_file_format;
    $$;
    EXECUTE IMMEDIATE 
        $$insert into $$ || dl_db || $$.$$ || dl_schema || $$.FORMS_RAW_STAGE(domain, metadata, metadata_filename, json, id, SYSTEM_QUERY_TS, task_id, execution_id)
        select
         f.value:domain::string domain,
         s.$1:meta metadata,
         metadata$filename metadata_filename,
         f.value JSON,
         f.value:id::string id,
         replace(split_part(metadata_filename, ''_'', -1), ''.json'') SYSTEM_QUERY_TS,
         $$ || task_id || $$ task_id,
         $$ || exec_id || $$ execution_id
        from @$$ || dl_db || $$.$$ || dl_schema || $$.s3_json_stage_daily_form s, lateral flatten(s.$1:objects) f
        qualify row_number() over (partition by id order by metadata$file_last_modified desc) = 1
        ;$$;
        
    --do locations
    EXECUTE IMMEDIATE ''TRUNCATE TABLE '' || dl_db || ''.'' || dl_schema || ''.LOCATIONS_RAW_STAGE;'';
    s3_path := :s3_path_root || ''location'' || :s3_path_time;
    EXECUTE IMMEDIATE 
        $$CREATE OR REPLACE STAGE $$ || dl_db || $$.$$ || dl_schema || $$.s3_json_stage_daily_location
            url=''$$ || s3_path || $$''
            Storage_integration = s3_int_obj
            file_format = $$ || dl_db || $$.$$ || dl_schema || $$.json_file_format;
    $$;
    EXECUTE IMMEDIATE 
        $$insert into $$ || dl_db || $$.$$ || dl_schema || $$.LOCATIONS_RAW_STAGE(domain, metadata, metadata_filename, json, id, SYSTEM_QUERY_TS, task_id, execution_id)
        select
         ''$$ || domain || $$'' domain,
         s.$1:meta metadata,
         metadata$filename metadata_filename,
         f.value JSON,
         f.value:id::string id,
         replace(split_part(metadata_filename, ''_'', -2), ''.json'') SYSTEM_QUERY_TS,
         $$ || task_id || $$ task_id,
         $$ || exec_id || $$ execution_id
        from @$$ || dl_db || $$.$$ || dl_schema || $$.s3_json_stage_daily_location s, lateral flatten(s.$1:objects) f
        qualify row_number() over (partition by id order by metadata$file_last_modified desc) = 1
        ;$$;
        
    --do fixtures
    EXECUTE IMMEDIATE ''TRUNCATE TABLE '' || dl_db || ''.'' || dl_schema || ''.FIXTURES_RAW_STAGE;'';
    s3_path := :s3_path_root || ''fixture'' || :s3_path_time;
    EXECUTE IMMEDIATE 
        $$CREATE OR REPLACE STAGE $$ || dl_db || $$.$$ || dl_schema || $$.s3_json_stage_daily_fixture
            url=''$$ || s3_path || $$''
            Storage_integration = s3_int_obj
            file_format = $$ || dl_db || $$.$$ || dl_schema || $$.json_file_format;
    $$;
    EXECUTE IMMEDIATE 
        $$insert into $$ || dl_db || $$.$$ || dl_schema || $$.FIXTURES_RAW_STAGE(domain, metadata, metadata_filename, json, id, SYSTEM_QUERY_TS, task_id, execution_id)
        select
         coalesce(f.value:fields.domain::string, ''$$ || domain || $$'') domain,
         s.$1:meta metadata,
         metadata$filename metadata_filename,
         f.value JSON,
         f.value:id::string id,
         replace(split_part(metadata_filename, ''_'', -2), ''.json'') SYSTEM_QUERY_TS,
         $$ || task_id || $$ task_id,
         $$ || exec_id || $$ execution_id
        from @$$ || dl_db || $$.$$ || dl_schema || $$.s3_json_stage_daily_fixture s, lateral flatten(s.$1:objects) f
        qualify row_number() over (partition by id order by metadata$file_last_modified desc) = 1
        ;$$;
        
    --do web users
    EXECUTE IMMEDIATE ''TRUNCATE TABLE '' || dl_db || ''.'' || dl_schema || ''.WEB_USERS_RAW_STAGE;'';
    s3_path := :s3_path_root || ''web-user'' || :s3_path_time;
    EXECUTE IMMEDIATE 
        $$CREATE OR REPLACE STAGE $$ || dl_db || $$.$$ || dl_schema || $$.s3_json_stage_daily_web_user
            url=''$$ || s3_path || $$''
            Storage_integration = s3_int_obj
            file_format = $$ || dl_db || $$.$$ || dl_schema || $$.json_file_format;
    $$;
    EXECUTE IMMEDIATE 
        $$insert into $$ || dl_db || $$.$$ || dl_schema || $$.WEB_USERS_RAW_STAGE(domain, metadata, metadata_filename, json, id, SYSTEM_QUERY_TS, task_id, execution_id)
        select
         ''$$ || domain || $$'' domain,
         s.$1:meta metadata,
         metadata$filename metadata_filename,
         f.value JSON,
         f.value:id::string id,
         replace(split_part(metadata_filename, ''_'', -2), ''.json'') SYSTEM_QUERY_TS,
         $$ || task_id || $$ task_id,
         $$ || exec_id || $$ execution_id
        from @$$ || dl_db || $$.$$ || dl_schema || $$.s3_json_stage_daily_web_user s, lateral flatten(s.$1:objects) f
        qualify row_number() over (partition by id order by metadata$file_last_modified desc) = 1
        ;$$;
        
    --do action times
    EXECUTE IMMEDIATE ''TRUNCATE TABLE '' || dl_db || ''.'' || dl_schema || ''.ACTION_TIMES_RAW_STAGE;'';
    s3_path := :s3_path_root || ''action_times'' || :s3_path_time;
    EXECUTE IMMEDIATE 
        $$CREATE OR REPLACE STAGE $$ || dl_db || $$.$$ || dl_schema || $$.s3_json_stage_daily_action_times
            url=''$$ || s3_path || $$''
            Storage_integration = s3_int_obj
            file_format = $$ || dl_db || $$.$$ || dl_schema || $$.json_file_format;
    $$;
    EXECUTE IMMEDIATE 
        $$insert into $$ || dl_db || $$.$$ || dl_schema || $$.ACTION_TIMES_RAW_STAGE(domain, metadata, metadata_filename, json, id, SYSTEM_QUERY_TS, task_id, execution_id)
        select
         ''$$ || domain || $$'' domain,
         s.$1:meta metadata,
         metadata$filename metadata_filename,
         f.value JSON,
         coalesce(f.value:user_id::string, f.value:user::string) || ''_'' || f.value:UTC_start_time::string id,
         replace(split_part(metadata_filename, ''_'', -2), ''.json'') SYSTEM_QUERY_TS,
         $$ || task_id || $$ task_id,
         $$ || exec_id || $$ execution_id
        from @$$ || dl_db || $$.$$ || dl_schema || $$.s3_json_stage_daily_action_times s, lateral flatten(s.$1:objects) f
        qualify row_number() over (partition by id order by metadata$file_last_modified desc) = 1
        ;$$;

    --Log Success
    EXECUTE IMMEDIATE ''UPDATE '' || dm_db || ''.UTIL.EXECUTION_LOG SET STATUS = \\''SUCCESS\\'', EXECUTION_END = SYSDATE() WHERE EXECUTION_ID = '' || exec_id || '';'';
    EXECUTE IMMEDIATE 
    $$insert into $$ || dm_db || $$.util.sql_logs (TASK_ID,EXECUTION_ID,QUERY_ID,QUERY_TEXT,DATABASE_NAME,SCHEMA_NAME,QUERY_TYPE,SESSION_ID,USER_NAME,ROLE_NAME,WAREHOUSE_NAME,WAREHOUSE_SIZE,WAREHOUSE_TYPE,CLUSTER_NUMBER,QUERY_TAG,EXECUTION_STATUS,ERROR_CODE,ERROR_MESSAGE,START_TIME,END_TIME,TOTAL_ELAPSED_TIME,BYTES_SCANNED,ROWS_PRODUCED,COMPILATION_TIME,EXECUTION_TIME,QUEUED_PROVISIONING_TIME,QUEUED_REPAIR_TIME,QUEUED_OVERLOAD_TIME,TRANSACTION_BLOCKED_TIME,OUTBOUND_DATA_TRANSFER_CLOUD,OUTBOUND_DATA_TRANSFER_REGION,OUTBOUND_DATA_TRANSFER_BYTES,INBOUND_DATA_TRANSFER_CLOUD,INBOUND_DATA_TRANSFER_REGION,INBOUND_DATA_TRANSFER_BYTES,CREDITS_USED_CLOUD_SERVICES,LIST_EXTERNAL_FILE_TIME,RELEASE_VERSION,EXTERNAL_FUNCTION_TOTAL_INVOCATIONS,EXTERNAL_FUNCTION_TOTAL_SENT_ROWS,EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS,EXTERNAL_FUNCTION_TOTAL_SENT_BYTES,EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES,IS_CLIENT_GENERATED_STATEMENT,QUERY_HASH,QUERY_HASH_VERSION,QUERY_PARAMETERIZED_HASH,QUERY_PARAMETERIZED_HASH_VERSION)
    select $$ || task_id || $$ TASK_ID, $$ || exec_id || $$ EXECUTION_ID, 
    * from table(information_schema.query_history_by_session(RESULT_LIMIT => 10000)) where execution_status<>''RUNNING'' and convert_timezone(''UTC'', START_TIME)::timestamp_ntz >= ''$$ || start_dt || $$''::timestamp_ntz
    order by start_time
    ;$$;
    RETURN ''Finished executing procedure, exec id: '' || exec_id;
    
EXCEPTION
  WHEN statement_error THEN 
      EXECUTE IMMEDIATE ''INSERT INTO '' || dm_db || ''.UTIL.MESSAGE_LOG (TASK_ID, EXECUTION_ID, TYPE, SUBTYPE, MESSAGE) SELECT '' || task_id || '', '' || exec_id || '', \\''ERROR\\'', \\''STATEMENT_ERROR\\'', OBJECT_CONSTRUCT(\\''ERROR_TYPE\\'', \\''STATEMENT_ERROR\\'', \\''SQLCODE\\'', \\'''' || sqlcode || ''\\'', \\''SQLERRM\\'', \\'''' || replace(sqlerrm, ''\\'''', ''\\\\\\'''') || ''\\'', \\''SQLSTATE\\'', \\'''' || sqlstate || ''\\'');'';
      EXECUTE IMMEDIATE ''UPDATE '' || dm_db || ''.UTIL.EXECUTION_LOG SET STATUS = \\''FAILURE\\'', EXECUTION_END = SYSDATE() WHERE EXECUTION_ID = '' || exec_id || '';'';
      EXECUTE IMMEDIATE 
      $$insert into $$ || dm_db || $$.util.sql_logs (TASK_ID,EXECUTION_ID,QUERY_ID,QUERY_TEXT,DATABASE_NAME,SCHEMA_NAME,QUERY_TYPE,SESSION_ID,USER_NAME,ROLE_NAME,WAREHOUSE_NAME,WAREHOUSE_SIZE,WAREHOUSE_TYPE,CLUSTER_NUMBER,QUERY_TAG,EXECUTION_STATUS,ERROR_CODE,ERROR_MESSAGE,START_TIME,END_TIME,TOTAL_ELAPSED_TIME,BYTES_SCANNED,ROWS_PRODUCED,COMPILATION_TIME,EXECUTION_TIME,QUEUED_PROVISIONING_TIME,QUEUED_REPAIR_TIME,QUEUED_OVERLOAD_TIME,TRANSACTION_BLOCKED_TIME,OUTBOUND_DATA_TRANSFER_CLOUD,OUTBOUND_DATA_TRANSFER_REGION,OUTBOUND_DATA_TRANSFER_BYTES,INBOUND_DATA_TRANSFER_CLOUD,INBOUND_DATA_TRANSFER_REGION,INBOUND_DATA_TRANSFER_BYTES,CREDITS_USED_CLOUD_SERVICES,LIST_EXTERNAL_FILE_TIME,RELEASE_VERSION,EXTERNAL_FUNCTION_TOTAL_INVOCATIONS,EXTERNAL_FUNCTION_TOTAL_SENT_ROWS,EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS,EXTERNAL_FUNCTION_TOTAL_SENT_BYTES,EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES,IS_CLIENT_GENERATED_STATEMENT,QUERY_HASH,QUERY_HASH_VERSION,QUERY_PARAMETERIZED_HASH,QUERY_PARAMETERIZED_HASH_VERSION)
    select $$ || task_id || $$ TASK_ID, $$ || exec_id || $$ EXECUTION_ID, 
    * from table(information_schema.query_history_by_session(RESULT_LIMIT => 10000)) where execution_status<>''RUNNING'' and convert_timezone(''UTC'', START_TIME)::timestamp_ntz >= ''$$ || start_dt || $$''::timestamp_ntz
    order by start_time
    ;$$;
      RETURN ''Error executing procedure, exec id: '' || exec_id;
  WHEN expression_error THEN
      EXECUTE IMMEDIATE ''INSERT INTO '' || dm_db || ''.UTIL.MESSAGE_LOG (TASK_ID, EXECUTION_ID, TYPE, SUBTYPE, MESSAGE) SELECT '' || task_id || '', '' || exec_id || '', \\''ERROR\\'', \\''EXPRESSION_ERROR\\'', OBJECT_CONSTRUCT(\\''ERROR_TYPE\\'', \\''EXPRESSION_ERROR\\'', \\''SQLCODE\\'', \\'''' || sqlcode || ''\\'', \\''SQLERRM\\'', \\'''' || replace(sqlerrm, ''\\'''', ''\\\\\\'''') || ''\\'', \\''SQLSTATE\\'', \\'''' || sqlstate || ''\\'');'';
      EXECUTE IMMEDIATE ''UPDATE '' || dm_db || ''.UTIL.EXECUTION_LOG SET STATUS = \\''FAILURE\\'', EXECUTION_END = SYSDATE() WHERE EXECUTION_ID = '' || exec_id || '';'';
      EXECUTE IMMEDIATE 
      $$insert into $$ || dm_db || $$.util.sql_logs (TASK_ID,EXECUTION_ID,QUERY_ID,QUERY_TEXT,DATABASE_NAME,SCHEMA_NAME,QUERY_TYPE,SESSION_ID,USER_NAME,ROLE_NAME,WAREHOUSE_NAME,WAREHOUSE_SIZE,WAREHOUSE_TYPE,CLUSTER_NUMBER,QUERY_TAG,EXECUTION_STATUS,ERROR_CODE,ERROR_MESSAGE,START_TIME,END_TIME,TOTAL_ELAPSED_TIME,BYTES_SCANNED,ROWS_PRODUCED,COMPILATION_TIME,EXECUTION_TIME,QUEUED_PROVISIONING_TIME,QUEUED_REPAIR_TIME,QUEUED_OVERLOAD_TIME,TRANSACTION_BLOCKED_TIME,OUTBOUND_DATA_TRANSFER_CLOUD,OUTBOUND_DATA_TRANSFER_REGION,OUTBOUND_DATA_TRANSFER_BYTES,INBOUND_DATA_TRANSFER_CLOUD,INBOUND_DATA_TRANSFER_REGION,INBOUND_DATA_TRANSFER_BYTES,CREDITS_USED_CLOUD_SERVICES,LIST_EXTERNAL_FILE_TIME,RELEASE_VERSION,EXTERNAL_FUNCTION_TOTAL_INVOCATIONS,EXTERNAL_FUNCTION_TOTAL_SENT_ROWS,EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS,EXTERNAL_FUNCTION_TOTAL_SENT_BYTES,EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES,IS_CLIENT_GENERATED_STATEMENT,QUERY_HASH,QUERY_HASH_VERSION,QUERY_PARAMETERIZED_HASH,QUERY_PARAMETERIZED_HASH_VERSION)
    select $$ || task_id || $$ TASK_ID, $$ || exec_id || $$ EXECUTION_ID, 
    * from table(information_schema.query_history_by_session(RESULT_LIMIT => 10000)) where execution_status<>''RUNNING'' and convert_timezone(''UTC'', START_TIME)::timestamp_ntz >= ''$$ || start_dt || $$''::timestamp_ntz
    order by start_time
    ;$$;
      RETURN ''Error executing procedures, exec id: '' || exec_id;
  WHEN OTHER THEN
      EXECUTE IMMEDIATE ''INSERT INTO '' || dm_db || ''.UTIL.MESSAGE_LOG (TASK_ID, EXECUTION_ID, TYPE, SUBTYPE, MESSAGE) SELECT '' || task_id || '', '' || exec_id || '', \\''ERROR\\'', \\''OTHER_ERROR\\'', OBJECT_CONSTRUCT(\\''ERROR_TYPE\\'', \\''OTHER_ERROR\\'', \\''SQLCODE\\'', \\'''' || sqlcode || ''\\'', \\''SQLERRM\\'', \\'''' || replace(sqlerrm, ''\\'''', ''\\\\\\'''') || ''\\'', \\''SQLSTATE\\'', \\'''' || sqlstate || ''\\'');'';
      EXECUTE IMMEDIATE ''UPDATE '' || dm_db || ''.UTIL.EXECUTION_LOG SET STATUS = \\''FAILURE\\'', EXECUTION_END = SYSDATE() WHERE EXECUTION_ID = '' || exec_id || '';'';
      EXECUTE IMMEDIATE 
      $$insert into $$ || dm_db || $$.util.sql_logs (TASK_ID,EXECUTION_ID,QUERY_ID,QUERY_TEXT,DATABASE_NAME,SCHEMA_NAME,QUERY_TYPE,SESSION_ID,USER_NAME,ROLE_NAME,WAREHOUSE_NAME,WAREHOUSE_SIZE,WAREHOUSE_TYPE,CLUSTER_NUMBER,QUERY_TAG,EXECUTION_STATUS,ERROR_CODE,ERROR_MESSAGE,START_TIME,END_TIME,TOTAL_ELAPSED_TIME,BYTES_SCANNED,ROWS_PRODUCED,COMPILATION_TIME,EXECUTION_TIME,QUEUED_PROVISIONING_TIME,QUEUED_REPAIR_TIME,QUEUED_OVERLOAD_TIME,TRANSACTION_BLOCKED_TIME,OUTBOUND_DATA_TRANSFER_CLOUD,OUTBOUND_DATA_TRANSFER_REGION,OUTBOUND_DATA_TRANSFER_BYTES,INBOUND_DATA_TRANSFER_CLOUD,INBOUND_DATA_TRANSFER_REGION,INBOUND_DATA_TRANSFER_BYTES,CREDITS_USED_CLOUD_SERVICES,LIST_EXTERNAL_FILE_TIME,RELEASE_VERSION,EXTERNAL_FUNCTION_TOTAL_INVOCATIONS,EXTERNAL_FUNCTION_TOTAL_SENT_ROWS,EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS,EXTERNAL_FUNCTION_TOTAL_SENT_BYTES,EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES,IS_CLIENT_GENERATED_STATEMENT,QUERY_HASH,QUERY_HASH_VERSION,QUERY_PARAMETERIZED_HASH,QUERY_PARAMETERIZED_HASH_VERSION)
    select $$ || task_id || $$ TASK_ID, $$ || exec_id || $$ EXECUTION_ID, 
    * from table(information_schema.query_history_by_session(RESULT_LIMIT => 10000)) where execution_status<>''RUNNING'' and convert_timezone(''UTC'', START_TIME)::timestamp_ntz >= ''$$ || start_dt || $$''::timestamp_ntz
    order by start_time
    ;$$;
      RETURN ''Error executing procedures, exec id: '' || exec_id;
END

{% endmacro %}