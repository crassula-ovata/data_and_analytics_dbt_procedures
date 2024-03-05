{% macro create_sp_s3_ingest() %}
CREATE OR REPLACE PROCEDURE 

    {% if target.name=='dev' %}
      METADATA.PROCEDURES_DEV
    {% elif target.name=='qa' %}
      METADATA.PROCEDURES_QA
    {% elif target.name=='prod' %}
      METADATA.PROCEDURES
    {% else %}
      invalid
    {% endif %}

.SP_S3_INGEST("TYPE" VARCHAR(16777216), "SUBTYPE" VARCHAR(16777216), "DOMAIN" VARCHAR(16777216), 
              "DL_DB" VARCHAR(16777216), "DL_SCHEMA" VARCHAR(16777216), "DM_DB" VARCHAR(16777216), 
              "TASK_ID" NUMBER(38,0), "PATH_TIME_OVERRIDE" VARCHAR(16777216), "SOURCES" VARCHAR(16777216))
    RETURNS VARCHAR(16777216)
    LANGUAGE SQL
    EXECUTE AS CALLER
AS '{{ sp_s3_ingest() }}';
{% endmacro %}
