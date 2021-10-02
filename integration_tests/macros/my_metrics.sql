{% macro re_data_metric_diff(column_name) %}
    max({{column_name}}) - min({{column_name}})
{% endmacro %}


{% macro re_data_metric_buy_count(time_column) %}
    coalesce(
        sum(
            case when event_type = 'buy'
                then 1
            else 0
            end
        ), 0
    )
{% endmacro %}

{% macro re_data_metric_my_custom_table_metric(time_column) %}
    1000
{% endmacro %}


{% macro re_data_metric_distinct_count(column_name) %}
    count(distinct( {{column_name}} ))

{% endmacro %}

{% macro re_data_metric_regex_test(column_name, config) %}
    {{ regex_test(column_name, config) }}
{% endmacro %}

{% macro regex_test(column_name, config) %}
    {% set pattern = config.get('regex') %}
    coalesce(
        sum(
            case when {{ re_data.regex_match_expression(column_name, pattern) }}
                then 1
            else 0
            end
        ), 0
    )
{% endmacro %}

