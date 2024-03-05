{% macro create_sp_s3_data_unload_gcp() %}
CREATE OR REPLACE PROCEDURE 

    {% if target.name=='dev-gcp' %}
      METADATA.PROCEDURES_DEV
    {% elif target.name=='qa-gcp' %}
      METADATA.PROCEDURES_QA
    {% elif target.name=='prod-gcp' %}
      METADATA.PROCEDURES
    {% else %}
      invalid
    {% endif %}

.SP_S3_DATA_UNLOAD("TYPE" VARCHAR(16777216), "SUBTYPE" VARCHAR(16777216), "PAYLOAD_NAME" VARCHAR(16777216), 
                   "DOMAIN" VARCHAR(16777216), "DL_DB" VARCHAR(16777216), "DL_SCHEMA" VARCHAR(16777216), 
                   "DM_DB" VARCHAR(16777216), "TASK_ID" NUMBER(38,0), "PATH_TIME_OVERRIDE" VARCHAR(16777216))
    RETURNS VARCHAR(16777216)
    LANGUAGE SQL
    EXECUTE AS CALLER
AS '{{ sp_s3_data_unload_gcp() }}';
{% endmacro %}
