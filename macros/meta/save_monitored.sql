{% macro save_monitored(monitored_path) %}

    {% set monitored_query %}
        select
            {{ full_table_name('name', 'schema', 'database') }} as {{ re_data.quote_column('model') }},
             time_filter as {{ re_data.quote_column('time_filter') }},
            metrics as {{ re_data.quote_column('metrics') }},
            columns as {{ re_data.quote_column('columns') }},
            anomaly_detector as {{ re_data.quote_column('anomaly_detector') }},
            owners as {{ re_data.quote_column('owners') }}
        from {{ ref('re_data_selected') }}
    {% endset %}
    {% set query_result = run_query(monitored_query) %}
    {% set monitored_file_path = monitored_path or 'target/re_data/monitored.json' %}
    {% do query_result.to_json(monitored_file_path) %}

{% endmacro %}