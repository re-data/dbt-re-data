{% macro export_alerts(start_date, end_date) %}
    {% set alerts_query %}
        select
            *
        from {{ ref('re_data_alerts') }}
        where
            case
                when type = 'anomaly' then time_window_end between '{{ start_date }}' and '{{ end_date }}'
                else true
            end
    {% endset %}

    {% set query_result = run_query(alerts_query) %}
    {% do query_result.to_json('target/re_data/alerts.json') %}
{% endmacro %}