
{% macro save_test_history(results) %}
    -- depends_on: {{ ref('re_data_test_history') }}

    {% if execute and results %}
        {% set to_insert = [] %}
        {% set run_started_at_str = run_started_at.strftime('%Y-%m-%d %H:%M:%S') %}

        {% for el in results %}
            {% if el.node.resource_type.name == 'Test' %}
                {% set any_refs = modules.re.findall("ref\(\'(?P<name>.*)\'\)", el.node.test_metadata.kwargs['model']) %}
                {% set any_source = modules.re.findall("source\(\'(?P<one>.*)\'\,\s+\'(?P<two>.*)\'\)", el.node.test_metadata.kwargs['model']) %}

                {% if any_refs %}
                    {% set name = any_refs[0] %}
                    {% set node_name = re_data.priv_full_name_from_depends(el.node, name) %}
                    {% set schema = graph.nodes.get(node_name)['schema'] %}
                    {% set database = graph.nodes.get(node_name)['database'] %}

                    {% set name = database + '.' + schema + '.' + name %} 
                    
                {% elif any_source %}
                    {% set package_name = any_source[0][0] %}
                    {% set name = any_source[0][1] %}
                    {% set node_name = re_data.priv_full_name_from_depends(el.node, name) %}
                    {% set schema = graph.sources.get(node_name)['schema'] %}
                    {% set database = graph.sources.get(node_name)['database'] %}

                    {% set name = database + '.' + schema + '.' + name %}
                {% else %}
                    {% set name = none %}
                {% endif %}

                {% do to_insert.append({ 'table_name': name, 'column_name': el.node.column_name or none , 'test_name': el.node.name, 'status': el.status.name, 'run_at': run_started_at_str}) %}
            {% endif %}
        {% endfor %}

        {% do re_data.insert_list_to_table(
            ref('re_data_test_history'),
            to_insert,
            ['table_name', 'column_name', 'test_name', 'status', 'run_at']
        ) %}

    {% endif %}

    {{ return ('') }}

{% endmacro %}

{% macro priv_full_name_from_depends(node, name) %}

    {% for full_name in node.depends_on.nodes %}
        {% set node_name = full_name.split('.')[-1] %}
        {% if node_name == name %}
            {{ return(full_name) }}
        {% endif %}
    {% endfor %}

    {{ return(none) }}

{% endmacro %}