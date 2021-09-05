{% macro get_tables() %}
    select table_name, time_filter
    from {{ ref('re_data_monitored') }}
{% endmacro %}