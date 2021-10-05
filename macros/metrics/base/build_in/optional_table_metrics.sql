{% macro re_data_metric_distinct_rows(context) %}
    (   with temp_table AS (
            select distinct * from {{ context.table_name }}
            where {{ in_time_window(context.time_filter) }}
        )
        select count(*) FROM temp_table
    )
{% endmacro %}
