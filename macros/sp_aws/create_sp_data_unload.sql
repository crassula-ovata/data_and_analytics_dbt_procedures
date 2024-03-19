{% macro create_sp_data_unload() %}
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

.SP_DATA_UNLOAD("TYPE" VARCHAR(16777216), "SUBTYPE" VARCHAR(16777216), "DOMAIN" VARCHAR(16777216), 
                "DL_DB" VARCHAR(16777216), "DL_SCHEMA" VARCHAR(16777216), "DM_DB" VARCHAR(16777216), 
                "STEPS" VARCHAR(16777216), "PATH_TIME_OVERRIDE" VARCHAR(16777216))
    RETURNS VARCHAR(16777216)
    LANGUAGE SQL
    EXECUTE AS CALLER
AS '{{ sp_data_unload() }}';
{% endmacro %}

