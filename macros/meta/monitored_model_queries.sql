{% macro get_tables() %}
    select *
    from {{ ref('re_data_monitored') }}
    order by table_name, time_filter
{% endmacro %}

{% macro get_schemas() %}
    select distinct schema, database
    from {{ ref('re_data_monitored') }}
{% endmacro %}