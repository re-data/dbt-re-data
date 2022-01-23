{% macro pub_insert_into_re_data_monitored() %}
    {% set monitored = re_data.pub_monitored_from_graph() %}
    
    {% do insert_list_to_table(
        this,
        monitored,
        ['name', 'schema', 'database', 'time_filter', 'metrics', 'columns']
    ) %}

    {% set monitored_set = {} %}
    {% for el in monitored %}
        {% do monitored_set.update({el['database'] + '.' + el['schema'] + '.' + el['name']: true}) %}
    {% endfor %}

    {% set monitored_vars = re_data.pub_monitored_from_vars() %}
    {% set not_yet_monitored = [] %}
    {% for el in monitored_vars %}
        {% if not monitored_set.get(el['database'] + '.' + el['schema'] + '.'  + el['name']) %}
            {% do not_yet_monitored.append(el) %}
        {% endif %}
    {% endfor %}

    {% do insert_list_to_table(
        this,
        not_yet_monitored,
        ['name', 'schema', 'database', 'time_filter', 'metrics', 'columns']
    ) %}

    {{ return('') }}

{% endmacro %}