{% macro insert_into_monitored() %}
    {% set monitored = re_data.pub_monitored_from_graph() %}
    {% set monitored_vars = re_data.pub_monitored_from_vars() %}

    {% do insert_list_to_table(
        ref('re_data_monitored'),
        monitored,
        ['full_name', 'name', 'schema', 'database', 'time_filter', 'metrics', 'columns']
    ) %}

    {% do insert_list_to_table(
        ref('re_data_monitored'),
        monitored_vars,
        ['full_name', 'name', 'schema', 'database', 'time_filter', 'metrics', 'columns']
    ) %}

    {{ return('') }}

{% macro %}