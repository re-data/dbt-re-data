
{% macro re_data_metric_row_count(context) %}
    count(1)
{% endmacro %}

{% macro re_data_metric_freshness(context) %}
    {{ freshness_expression(context.time_filter) }}
{% endmacro %}

{% macro freshness_expression(time_filter) %}
    {{ adapter.dispatch('freshness_expression', 're_data')(time_filter) }}
{% endmacro %}

{% macro default__freshness_expression(time_filter) %}
    EXTRACT(EPOCH FROM ({{time_window_end()}} - max({{time_filter}})))
{% endmacro %}

{% macro bigquery__freshness_expression(time_filter) %}
    TIMESTAMP_DIFF ( timestamp({{ time_window_end()}}), timestamp(max({{time_filter}})), SECOND)
{% endmacro %}

{% macro snowflake__freshness_expression(time_filter) %}
    timediff(second, max({{time_filter}}), {{- time_window_end() -}})
{% endmacro %}

{% macro redshift__freshness_expression(time_filter) %}
    DATEDIFF(second, max({{time_filter}}), {{- time_window_end() -}})
{% endmacro %}
