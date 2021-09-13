
{% macro metric_row_count(time_filter) %}
    count(1)
{% endmacro %}

{% macro metric_freshness(time_filter) %}
    {{ freshness_expression(time_filter) }}
{% endmacro %}

{% macro freshness_expression(time_column) %}
    {{ adapter.dispatch('freshness_expression', 're_data')(time_column) }}
{% endmacro %}

{% macro default__freshness_expression(time_column) %}
   EXTRACT(EPOCH FROM ({{time_window_end()}} - max({{time_column}})))
{% endmacro %}

{% macro bigquery__freshness_expression(time_column) %}
    TIMESTAMP_DIFF ( timestamp({{ time_window_end()}}), timestamp(max({{time_column}})), SECOND)
{% endmacro %}

{% macro snowflake__freshness_expression(time_column) %}
   timediff(second, max({{time_column}}), {{- time_window_end() -}})
{% endmacro %}

{% macro redshift__freshness_expression(time_column) %}
   DATEDIFF(second, max({{time_column}}), {{- time_window_end() -}})
{% endmacro %}
