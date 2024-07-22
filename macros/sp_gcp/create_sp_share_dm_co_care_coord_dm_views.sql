{%- macro create_sp_share_dm_co_care_coord_dm_views() -%}
CREATE OR REPLACE PROCEDURE UTIL.REPLICATION.
    {%- if target.name=='test-gcp' -%}
SP_SHARE_DM_CO_CARE_COORD_TEST_DM_VIEWS
    {%- elif target.name=='dev-gcp' -%}
SP_SHARE_DM_CO_CARE_COORD_DEV_DM_VIEWS
    {%- elif target.name=='qa-gcp' -%}
SP_SHARE_DM_CO_CARE_COORD_QA_DM_VIEWS
    {%- elif target.name=='prod-gcp' -%}
SP_SHARE_DM_CO_CARE_COORD_PROD_DM_VIEWS
    {%- endif -%}
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
{{ sp_share_dm_care_coord_dm_views() }}
$$;
{%- endmacro -%}