{% macro get_tables() %}
    select *
    from {{ ref('re_data_monitored') }}
    order by table_name, time_filter
{% endmacro %}