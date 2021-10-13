{% macro re_data_metric_distinct_table_rows(context) %}
    with temp_table AS (
            select distinct * from {{ context.table_name }}
            where {{ in_time_window(context.time_filter) }}
        )
    select coalesce(count(*), 0) FROM temp_table
{% endmacro %}
