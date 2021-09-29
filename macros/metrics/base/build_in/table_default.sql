
{% macro re_data_metric_row_count(time_filter, config) %}
    count(1)
{% endmacro %}

{% macro re_data_metric_freshness(time_filter, config) %}
    {{ freshness_expression(time_filter) }}
{% endmacro %}

{% macro freshness_expression(time_column) %}
    {{ adapter.dispatch('freshness_expression', 're_data')(time_column) }}
{% endmacro %}

{% macro default__freshness_expression(time_column, config) %}
   EXTRACT(EPOCH FROM ({{time_window_end()}} - max({{time_column}})))
{% endmacro %}

{% macro bigquery__freshness_expression(time_column, config) %}
    TIMESTAMP_DIFF ( timestamp({{ time_window_end()}}), timestamp(max({{time_column}})), SECOND)
{% endmacro %}

{% macro snowflake__freshness_expression(time_column, config) %}
   timediff(second, max({{time_column}}), {{- time_window_end() -}})
{% endmacro %}

{% macro redshift__freshness_expression(time_column, config) %}
   DATEDIFF(second, max({{time_column}}), {{- time_window_end() -}})
{% endmacro %}
