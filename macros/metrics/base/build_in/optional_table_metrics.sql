{% macro re_data_metric_distinct_rows(context) %}
    {% set table_name = context.get('table_name') %}
    {% set time_filter = context.get('time_filter') %}
    (   with temp_table AS (
            select distinct * from {{ table_name }}
            where {{ in_time_window(time_filter) }}
        )
        select count(*) FROM temp_table
    )
{% endmacro %}
