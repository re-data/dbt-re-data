
{% macro time_filter(column_name, column_type) %}

    case when {{ is_datetime(column_type)}} = true
    then
        column_name
    else
        null
    end

{% endmacro %}


{% macro time_window_start() %}
    cast('{{- var('re_data:time_window_start') -}}' as timestamp) 
{% endmacro %}


{% macro time_window_end() %}
    cast('{{- var('re_data:time_window_end') -}}' as timestamp)
{% endmacro %}


{% macro anamaly_detection_time_window_start() %}
   cast('{{- var('re_data:anomaly_detection_window_start') -}}' as timestamp)
{% endmacro %}


{% macro freshness_expression(time_column) %}
    {{ adapter.dispatch('freshness_expression')(time_column) }}
{% endmacro %}

{% macro default__freshness_expression(time_column) %}
   EXTRACT(EPOCH FROM ({{time_window_end()}} - max({{time_column}})))
{% endmacro %}

{% macro bigquery__freshness_expression(time_column) %}
    TIMESTAMP_DIFF ( timestamp({{ time_window_end()}}), max({{time_column}}), SECOND)
{% endmacro %}

{% macro snowflake__freshness_expression(time_column) %}
   timediff(second, max({{time_column}}), {{- time_window_end() -}})
{% endmacro %}

{% macro redshift__freshness_expression(time_column) %}
   DATEDIFF(second, max({{time_column}}), {{- time_window_end() -}})
{% endmacro %}