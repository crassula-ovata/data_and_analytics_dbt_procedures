{%- macro create_sp_share_dm_co_care_coord_dm_views() -%}
CREATE OR REPLACE PROCEDURE METADATA.
    {%- if target.name=='test-gcp' %}
PROCEDURES_TEST
    {%- elif target.name=='dev-gcp' %}
PROCEDURES_DEV
    {%- elif target.name=='qa-gcp' %}
PROCEDURES_QA
    {%- elif target.name=='prod-gcp' %}
PROCEDURES
    {%- endif -%}
.SP_SHARE_DM_CO_CARE_COORD_DM_VIEWS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
{{ sp_share_dm_co_care_coord_dm_views() }}
$$;
{%- endmacro -%}
