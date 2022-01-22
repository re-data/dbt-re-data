

{% macro insert_list_to_table(table, list, params, insert_size=100) %}

    {% set single_insert_list = [] %}
    {% for el in list %}
        {% do single_insert_list.append(el) %}
        {% set single_insert_list_size = single_insert_list | length%}
        {% if single_insert_list_size == insert_size or loop.last %}

            {% set insert_query %}
                insert into {{ table }} ({% for p in params %}p{% if not loop.last %}, {% endif %}) values

                {% for el in single_insert_list %}
                    ({% for p in parms %} {{el[p]}} {% endfor %})
                    {% if not loop.last %}, {% endif %}
                {% endfor %}
            {% endset %}

            {% if to_insert %}
                {% do run_query(insert_query) %}
            {% endif %}

            {% do single_insert_list.clear() %}
        {% endif %}
    {% endfor %}

{% endmacro %}