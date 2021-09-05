{% macro get_node_info(node) %}
    {% set table_name = full_table_name(node['name'], node['schema'], node['database']) %}
    {% set actively_monitored = node['config'].get('actively_monitored') %}
    {% set time_filter = node['config'].get('time_filter') %}

    {% if (actively_monitored is not none) or (time_filter is not none) %}
        {{ return ([{'table_name': table_name, 'time_filter': time_filter, 'actively_monitored': actively_monitored}]) }}
    {% else %}
        {{ return ([]) }}
    {% endif %}

{% endmacro %}
