{% macro create_sp_run_job() %}
CREATE OR REPLACE PROCEDURE 

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

.SP_RUN_JOB("JOBNAME" VARCHAR(16777216), "SOURCESQL" VARCHAR(16777216), "TYPE" VARCHAR(16777216), 
            "SUBTYPE" VARCHAR(16777216), "DOMAIN" VARCHAR(16777216), "DL_DB" VARCHAR(16777216), 
            "DL_SCHEMA" VARCHAR(16777216), "DM_DB" VARCHAR(16777216), "TASK_ID" NUMBER(38,0))
    RETURNS VARCHAR(16777216)
    LANGUAGE SQL
    EXECUTE AS CALLER
AS '{{ sp_run_job() }}';
{% endmacro %}
