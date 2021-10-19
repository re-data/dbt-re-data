{% macro re_data_metric_diff(context) %}
    max({{context.column_name}}) - min({{context.column_name}})
{% endmacro %}

{% macro re_data_metric_my_custom_table_metric(context) %}
    1000
{% endmacro %}

{% macro re_data_metric_regex_test(context) %}
    {{ regex_test(context.column_name, context.config.regex) }}
{% endmacro %}

{% macro regex_test(column_name, pattern) %}
    coalesce(
        sum(
            case when {{ re_data.regex_match_expression(column_name, pattern) }}
                then 1
            else 0
            end
        ), 0
    )
{% endmacro %}

{% macro re_data_metric_my_distinct_table_rows(context) %}
    with temp_table AS (
            select distinct * from {{ context.table_name }}
        )
    select coalesce(count(*), 0) FROM temp_table
{% endmacro %}