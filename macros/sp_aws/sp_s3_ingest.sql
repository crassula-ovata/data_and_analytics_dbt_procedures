{% macro sp_s3_ingest() %}

DECLARE
    exec_id integer;
    uuid default uuid_string();
    start_dt default SYSDATE();
    s3_path_root string;
    s3_path_time string default coalesce(path_time_override, to_char(dateadd(''HH'', -1, sysdate()), ''/YYYY/MM/DD/HH/''));
    s3_path string;
    s3_regex string;
    s3_query_id string;
    steps resultset;
    step_sql string;
BEGIN
    --create exec log record
    EXECUTE IMMEDIATE ''INSERT INTO '' || dm_db || ''.UTIL.EXECUTION_LOG (TASK_ID, UUID, TYPE, SUBTYPE, DOMAIN, STATUS, EXECUTION_START) VALUES('' || task_id || '', \\'''' || uuid || ''\\'', \\'''' || type || ''\\'', \\'''' || subtype || ''\\'', \\'''' || domain || ''\\'', \\''START\\'', \\'''' || start_dt || ''\\''::timestamp_ntz);'';
    --get exec log id
    LET ref string := :dm_db || ''.UTIL.EXECUTION_LOG'';
    SELECT EXECUTION_ID INTO :exec_id FROM IDENTIFIER(:ref) WHERE UUID = :uuid and STATUS = ''START'';

    --get root s3 path for current project
    ref := ''@'' || :dl_db || ''.'' || :dl_schema || ''.s3_json_stage'';
    SELECT GET_STAGE_LOCATION(:ref) INTO :s3_path_root;

    --get regex to list files for sources with year/month/day/hour folder path
    select listagg(''(.*/'' || f.value::string || ''/[0-9]{4}/[0-9]{2}/[0-9]{2}/[0-9]{2}/.*)'', ''|'') 
        into :s3_regex from lateral flatten (split(:sources, ''|'')) f;

    --get files in stage for sources with expected folder path
    EXECUTE IMMEDIATE $$list @$$ || dl_db || $$.$$ || dl_schema || $$.S3_JSON_STAGE pattern = ''$$ || s3_regex || $$'';$$;

    --get last query id in order to use the file list in a query with calcs
    select last_query_id() into :s3_query_id;

    --get source steps configured
    steps := (EXECUTE IMMEDIATE $$with n as (
        select distinct split_part("name", ''/'', -6) type
            ,''/'' || split_part("name", ''/'', -5) || ''/'' || split_part("name", ''/'', -4) || ''/'' || 
                split_part("name", ''/'', -3) || ''/'' || split_part("name", ''/'', -2) || ''/'' path
        from table(result_scan(''$$ || s3_query_id || $$'')) 
    )
    , c as (
        select source_domain, source, last_ts, path, coalesce(domain_calc, ''\\''$$ || domain || $$\\'''') domain_calc, 
            meta_calc, json_calc, id_calc, ts_calc, flatten_calc
        from $$ || dl_db || $$.$$ || dl_schema || $$.VW_LAST_TS_BY_DOMAIN_SOURCE 
        where source in (select f.value::string from lateral flatten(split(''$$ || sources || $$'', ''|'')) f) 
            and ifnull(source_domain, ''$$ || domain || $$'') = ''$$ || domain || $$''
    )
    select n.type, n.path , c.source, c.domain_calc, c.meta_calc, c.json_calc, c.id_calc, c.ts_calc, c.flatten_calc 
    from n left join c on c.source = n.type 
    where $$ || case when path_time_override is null then $$n.path > ifnull(c.path, '''')$$ 
        else $$n.path >= ''$$ || ifnull(path_time_override, '''') || $$''$$ end || $$
    order by n.type, n.path
    ;$$);

    LET c cursor FOR steps;
    OPEN c;

    --loop through steps
    FOR step IN c DO
        --set up current source stage
        s3_path := :s3_path_root || step.source || step.path;
        EXECUTE IMMEDIATE 
            $$CREATE OR REPLACE STAGE $$ || dl_db || $$.$$ || dl_schema || $$.s3_json_stage_daily_$$ || replace(step.source, ''-'', ''_'') || $$
                url=''$$ || s3_path || $$''
                Storage_integration = s3_int_obj
                file_format = $$ || dl_db || $$.$$ || dl_schema || $$.json_file_format;
        $$;
        --insert into stage using current source parameters
        EXECUTE IMMEDIATE 
            $$merge into $$ || dl_db || $$.$$ || dl_schema || $$.$$ || upper(replace(regexp_replace(step.source, ''(.*)(s$)'', ''\\\\1''), ''-'', ''_'')) || $$S_RAW_STAGE T 
            USING (
            select
             $$ || step.domain_calc || $$ domain,
             $$ || step.meta_calc || $$ metadata,
             metadata$filename metadata_filename,
             $$ || step.json_calc || $$ JSON,
             $$ || step.id_calc || $$ id,
             $$ || step.ts_calc || $$ SYSTEM_QUERY_TS,
             $$ || task_id || $$ task_id,
             $$ || exec_id || $$ execution_id
            from @$$ || dl_db || $$.$$ || dl_schema || $$.s3_json_stage_daily_$$ || replace(step.source, ''-'', ''_'') || $$ s, lateral flatten($$ || step.flatten_calc || $$) f
            qualify row_number() over (partition by id order by metadata$file_last_modified desc) = 1
            ) AS S ON T.ID = S.ID 
            WHEN MATCHED THEN UPDATE SET T.DOMAIN=S.DOMAIN, T.JSON=S.JSON, T.ID=S.ID, T.SYSTEM_QUERY_TS = S.SYSTEM_QUERY_TS, 
                T.TASK_ID = S.TASK_ID, T.EXECUTION_ID = S.EXECUTION_ID, T.METADATA = S.METADATA, T.METADATA_FILENAME = S.METADATA_FILENAME 
            WHEN NOT MATCHED THEN INSERT(DOMAIN, JSON, ID, SYSTEM_QUERY_TS, TASK_ID, EXECUTION_ID, METADATA, METADATA_FILENAME) 
                VALUES(S.DOMAIN, S.JSON, S.ID, S.SYSTEM_QUERY_TS, S.TASK_ID, S.EXECUTION_ID, S.METADATA, S.METADATA_FILENAME);$$;
    END FOR;

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