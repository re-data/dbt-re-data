
{% macro time_filter(column_name, column_type) %}

    case when {{ re_data.is_datetime(column_type)}} = true
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
   {{ adapter.dispatch('anamaly_detection_time_window_start')() }}
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

{% macro freshness_expression(time_column) %}
    {{ adapter.dispatch('freshness_expression')(time_column) }}
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


{% macro interval_length_sec(start_timestamp, end_timestamp) %}
    {{ adapter.dispatch('interval_length_sec')(start_timestamp, end_timestamp) }}
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

{% macro before_time_window_end(time_column) %}
    {{ adapter.dispatch('before_time_window_end')(time_column) }}
{% endmacro %}

{% macro default__before_time_window_end(time_column) %}
    {{time_column}} < {{- time_window_end() -}}
{% endmacro %}

{% macro bigquery__before_time_window_end(time_column) %}
    timestamp({{time_column}}) < {{- time_window_end() -}}
{% endmacro %}


{% macro in_time_window(time_column) %}
    {{ adapter.dispatch('in_time_window')(time_column) }}
{% endmacro %}

{% macro default__in_time_window(time_column) %}
    {{time_column}} >= {{ time_window_start() }} and
    {{time_column}} < {{ time_window_end() }}
{% endmacro %}

{% macro bigquery__in_time_window(time_column) %}
    timestamp({{time_column}}) >= {{ time_window_start() }} and
    timestamp({{time_column}}) < {{ time_window_end() }}
{% endmacro %}
