
{% macro store_results(results) %}
    {% set to_insert = [] %}

    {% if execute and results %}
        {% for el in results %}
            {% if el.node.resource_type.name == 'Test' %}
                {{ debug() }}
                {% do to_insert.append({'name': el.node.name, 'status': el.status.name, 'model': el.node.file_key_name, 'column': el.node.column_name}) %}
            {% endif %}
        {% endfor %}
    {% endif %}
    select 1

{% endmacro %}

