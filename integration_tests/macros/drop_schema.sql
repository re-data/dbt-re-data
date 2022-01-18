{% macro drop_schema(schema_name) %}
    {% set drop_query %}
        drop schema if exists {{schema_name}} cascade;
        drop schema if exists {{schema_name}}_re cascade;
        drop schema if exists {{schema_name}}_re_internal cascade;
        drop schema if exists {{schema_name}}_raw cascade;
        drop schema if exists {{schema_name}}_expected cascade;
        drop schema if exists {{schema_name}}_dbt_test__audit cascade;
    {% endset %}
    {% do run_query(drop_query) %}
{% endmacro %}