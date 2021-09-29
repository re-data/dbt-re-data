{% macro re_data_metric_diff(column_name, config) %}
    max({{column_name}}) - min({{column_name}})
{% endmacro %}


{% macro re_data_metric_buy_count(time_column, config) %}
    coalesce(
        sum(
            case when event_type = 'buy'
                then 1
            else 0
            end
        ), 0
    )
{% endmacro %}

{% macro re_data_metric_my_custom_table_metric(time_column, config) %}
    1000
{% endmacro %}


{% macro re_data_metric_distinct_count(column_name, config) %}
    count(distinct( {{column_name}} ))

{% endmacro %}

{% macro re_data_metric_regexp_test(column_name, config) %}
    {% set pattern = config.get('regexp') %}
    coalesce(
        sum(
            case when event_type ~ '{{pattern}}'
                then 1
            else 0
            end
        ), 0
    )

{% endmacro %}
