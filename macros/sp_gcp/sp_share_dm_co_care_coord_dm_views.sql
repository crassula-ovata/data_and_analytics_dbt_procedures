{%- macro sp_share_dm_co_care_coord_dm_views() -%}
    {%- if target.name=='test-gcp' -%}
        {%- set db_name = 'dm_co_care_coord_test' -%}
        {%- set share_name = 'bha_uat_s' -%}
    {%- elif target.name=='dev-gcp' -%}
        {%- set db_name = 'dm_co_care_coord_dev' -%}
        {%- set share_name = 'bha_uat_s' -%}
    {%- elif target.name=='qa-gcp' -%}
        {%- set db_name = 'dm_co_care_coord_qa' -%}
        {%- set share_name = 'bha_uat_s' -%}
    {%- elif target.name=='prod-gcp' -%}
        {%- set db_name = 'dm_co_care_coord_prod' -%}
        {%- set share_name = 'bha_prod_s' -%}
    {%- endif -%}
BEGIN
    {%- if target.name=='test-gcp' or target.name=='dev-gcp' %}
    -- NOTE: DO NOT RUN - FOR TESTING ONLY
    {%- endif %}
    grant select on view {{db_name}}.dm.VW_ARCHIVED_CASES to share {{share_name}};
    grant select on view {{db_name}}.dm.VW_CLIENT_ALIAS to share {{share_name}};
    grant select on view {{db_name}}.dm.VW_CLINIC_PROVIDER to share {{share_name}};
    grant select on view {{db_name}}.dm.VW_EXPLODE_CLINIC_ACCESSIBILITY to share {{share_name}};
    grant select on view {{db_name}}.dm.VW_EXPLODE_CLINIC_CLINIC_TYPE to share {{share_name}};
    grant select on view {{db_name}}.dm.VW_EXPLODE_CLINIC_LANGUAGES_SPOKEN to share {{share_name}};
    grant select on view {{db_name}}.dm.VW_EXPLODE_CLINIC_MENTAL_HEALTH_SERVICES to share {{share_name}};
    grant select on view {{db_name}}.dm.VW_EXPLODE_CLINIC_PAYERS_ACCEPTED to share {{share_name}};
    grant select on view {{db_name}}.dm.VW_EXPLODE_CLINIC_POPULATION_SERVED to share {{share_name}};
    grant select on view {{db_name}}.dm.VW_EXPLODE_CLINIC_PROGRAMS to share {{share_name}};
    grant select on view {{db_name}}.dm.VW_EXPLODE_CLINIC_SUBSTANCE_USE_SERVICES to share {{share_name}};
    grant select on view {{db_name}}.dm.VW_FORM_METADATA to share {{share_name}};
    grant select on view {{db_name}}.dm.VW_SERVICE_CLINIC_PROVIDER_CLIENT to share {{share_name}};
END
{%- endmacro -%}