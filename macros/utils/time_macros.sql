
{% macro time_window_start() %}
    cast('{{- var('re_data:time_window_start') -}}' as timestamp) 
{% endmacro %}


{% macro time_window_end() %}
    cast('{{- var('re_data:time_window_end') -}}' as timestamp)
{% endmacro %}


{% macro anamaly_detection_time_window_start() %}
   {{ adapter.dispatch('anamaly_detection_time_window_start', 're_data')() }}
{% endmacro %}

{% macro default__anamaly_detection_time_window_start() %}
    {{ time_window_start() }} - interval '{{var('re_data:anomaly_detection_look_back_days')}} days'
{% endmacro %}

{% macro bigquery__anamaly_detection_time_window_start() %}
    DATE_ADD({{ time_window_start() }}, INTERVAL -{{var('re_data:anomaly_detection_look_back_days')}} DAY)
{% endmacro %}

{% macro snowflake__anamaly_detection_time_window_start() %}
    DATEADD('DAY', -{{-var('re_data:anomaly_detection_look_back_days')-}}, {{ time_window_start() }})
{% endmacro %}


{% macro interval_length_sec(start_timestamp, end_timestamp) %}
    {{ adapter.dispatch('interval_length_sec', 're_data')(start_timestamp, end_timestamp) }}
{% endmacro %}

{% macro default__interval_length_sec(start_timestamp, end_timestamp) %}
   EXTRACT(EPOCH FROM ({{ end_timestamp }} - {{ start_timestamp }} ))
{% endmacro %}

{% macro bigquery__interval_length_sec(start_timestamp, end_timestamp) %}
    TIMESTAMP_DIFF ({{ end_timestamp }}, {{ start_timestamp }}, SECOND)
{% endmacro %}

{% macro snowflake__interval_length_sec(start_timestamp, end_timestamp) %}
   timediff(second, {{ start_timestamp }}, {{ end_timestamp }})
{% endmacro %}

{% macro redshift__interval_length_sec(start_timestamp, end_timestamp) %}
   DATEDIFF(second, {{ start_timestamp }}, {{ end_timestamp }})
{% endmacro %}

{%- macro in_time_window(time_column) %}
    {# /* If not time_filter is specified, we compute the metric over the entire table else we filter for the time frame */ #}
    {% if time_column is none %}
            true
    {% else %}
        {{ adapter.dispatch('in_time_window', 're_data')(time_column) }}
    {% endif %}
{% endmacro -%}

{% macro default__in_time_window(time_column) %}
    {{time_column}} >= {{ time_window_start() }} and
    {{time_column}} < {{ time_window_end() }}
{% endmacro %}

{% macro bigquery__in_time_window(time_column) %}
    timestamp({{time_column}}) >= {{ time_window_start() }} and
    timestamp({{time_column}}) < {{ time_window_end() }}
{% endmacro %}


{% macro format_timestamp(column_name) %}
    {{ adapter.dispatch('format_timestamp', 're_data')(column_name) }}
{% endmacro %}

{% macro default__format_timestamp(column_name) %}
    to_char({{column_name}}, 'YYYY-MM-DD HH24:MI:SS')
{% endmacro %}

{% macro bigquery__format_timestamp(column_name) %}
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%I:%S', {{column_name}})
{% endmacro %}
