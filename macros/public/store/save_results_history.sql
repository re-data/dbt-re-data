
{% macro full_name_from_depends(node, name) %}

    {% for full_name in node.depends_on.nodes %}
        {% set node_name = full_name.split('.')[-1] %}
        {% if node_name == name %}
            {{ return(full_name) }}
        {% endif %}
    {% endfor %}

    {{ return(none) }}

{% endmacro %}

{% macro save_test_history(results) %}
    -- depends_on: {{ ref('re_data_test_history') }}

    {% if execute and results %}
        {% set to_insert = [] %}
        {% for el in results %}
            {% if el.node.resource_type.name == 'Test' %}
                {% set any_refs = modules.re.findall("ref\(\'(?P<name>.*)\'\)", el.node.test_metadata.kwargs['model']) %}
                {% set any_source = modules.re.findall("source\(\'(?P<one>.*)\'\,\s+\'(?P<two>.*)\'\)", el.node.test_metadata.kwargs['model']) %}

                {% if any_refs %}
                    {% set name = any_refs[0] %}
                    {% set node_name = re_data.full_name_from_depends(el.node, name) %}
                    {% set schema = graph.nodes.get(node_name)['schema'] %}
                    {% set database = graph.nodes.get(node_name)['database'] %}

                    {% set name = database + '.' + schema + '.' + name %} 
                    
                {% elif any_source %}
                    {% set package_name = any_source[0][0] %}
                    {% set name = any_source[0][1] %}
                    {% set node_name = re_data.full_name_from_depends(el.node, name) %}
                    {% set schema = graph.sources.get(node_name)['schema'] %}
                    {% set database = graph.sources.get(node_name)['database'] %}

                    {% set name = database + '.' + schema + '.' + name %}
                {% else %}
                    {% set name = none %}
                {% endif %}

                {% do to_insert.append({ 'table_name': name, 'column_name': el.node.column_name , 'test_name': el.node.name, 'status': el.status.name}) %}
            {% endif %}
        {% endfor %}

        {% set run_started_at_str = run_started_at.strftime('%Y-%m-%d %H:%M:%S') %}

        {% set single_insert_list = [] %}
        {% for el in to_insert %}
            {% do single_insert_list.append(el) %}
            {% set single_insert_list_size = single_insert_list | length%}
            {% if single_insert_list_size == 100 or loop.last %}

                {% set insert_query %}
                    insert into {{ ref('re_data_test_history')}} (table_name, column_name, test_name, status, run_at) values

                    {% for el in single_insert_list %}
                        ( '{{ el.table_name }}' , {% if el.column_name %}'{{ el.column_name }}'{% else %}null{% endif %}, '{{ el.test_name }}', '{{ el.status }}', '{{ run_started_at_str }}' ) {% if not loop.last %}, {% endif %}
                    {% endfor %}
                {% endset %}

                {% if to_insert %}
                    {% do run_query(insert_query) %}
                {% endif %}

                {% do single_insert_list.clear() %}
            {% endif %}
        {% endfor %}
    {% endif %}

    {{ return ('') }}

{% endmacro %}

