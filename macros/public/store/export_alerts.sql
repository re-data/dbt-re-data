-- depends_on: {{ ref('re_data_alerting') }}

{% macro export_alerts(start_date, end_date) %}
    {% set alerts_query %}
        select
            {{ overview_select_base('alert', 'computed_on')}}
            {{ to_single_json(['id', 'metric', 'z_score_value', 'last_value', 'last_avg', 'last_stddev', 'time_window_end', 'interval_length_sec']) }} as {{ re_data.quote_column('data') }}
        from
            {{ ref('re_data_alerting') }}
            where date(time_window_end) between '{{start_date}}' and '{{end_date}}'
    {% endset %}

    {% set query_result = run_query(alerts_query) %}
    {% do query_result.to_json('target/re_data/alerts.json') %}
{% endmacro %}