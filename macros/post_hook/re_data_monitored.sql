{% macro pub_insert_into_re_data_monitored() %}
    {% set monitored = re_data.pub_monitored_from_graph() %}
    {% do re_data.insert_list_to_table(
        this,
        monitored,
        ['name', 'schema', 'database', 'time_filter', 'metrics', 'columns', 'anomaly_detector', 'owners']
    ) %}

    {{ return('') }}

{% endmacro %}