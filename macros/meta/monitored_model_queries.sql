{% macro get_tables() %}
    select *
    from {{ ref('re_data_selected') }}
    order by name, schema, database, time_filter
{% endmacro %}

{% macro get_schemas() %}
    select distinct schema, database
    from {{ ref('re_data_selected') }}
{% endmacro %}