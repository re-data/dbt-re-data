{% macro export_alerts(start_date, end_date, alerts_path=None, monitored_path=None) %}
    {% set alerts_query %}
        select
            type as {{ re_data.quote_column('type') }},
            model as {{ re_data.quote_column('model') }},
            message as {{ re_data.quote_column('message') }},
            value as {{ re_data.quote_column('value') }},
            {{ format_timestamp('time_window_end')}} as {{ re_data.quote_column('time_window_end') }} 
        from {{ ref('re_data_alerts') }}
        where
            case
                when type = 'anomaly' then time_window_end between '{{ start_date }}' and '{{ end_date }}'
                else time_window_end >= '{{ start_date }}'
            end
        order by time_window_end desc
    {% endset %}

    {% set query_result = run_query(alerts_query) %}
    {% set alerts_file_path = alerts_path or 'target/re_data/alerts.json' %}
    {% do query_result.to_json(alerts_file_path) %}
    {{ save_monitored(monitored_path) }}
{% endmacro %}