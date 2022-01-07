
{% macro save_test_history(results) %}
    -- depends_on: {{ ref('re_data_test_history') }}

    {% set re = modules.re %}

    {% if execute and results %}
        {% set to_insert = [] %}
        {% for el in results %}
            {% if el.node.resource_type.name == 'Test' %}
                {% set any_refs = re.findall("ref\(\'(?P<name>.*)\'\)", el.node.test_metadata.kwargs['model']) %}
                {% set any_source = re.findall("source\(\'(?P<one>.*)\'\,\s+\'(?P<two>.*)\'\)", el.node.test_metadata.kwargs['model']) %}
                {% set package_name = el.node.package_name %}
                {% if any_refs %}
                    {% set name = any_refs[0] %}
                    {% set schema = re_data.graph_param('schema', name, package_name, ['seed', 'model']) %}
                    {% set database = re_data.graph_param('database', name, package_name, ['seed', 'model']) %}
                    {% set name = database + '.' + schema + '.' + name %} 
                {% elif any_source %}
                    {% set name = any_source[0][1] %}
                    {% set schema = re_data.graph_param('schema', name, package_name, ['source']) %}
                    {% set database = re_data.graph_param('database', name, package_name, ['source']) %}
                    {% set name = database + '.' + schema + '.' + name %} 
                {% else %}
                    {% set name = none %}
                {% endif %}

                {% do to_insert.append({ 'table_name': name, 'column_name': el.node.column_name , 'test_name': el.node.name, 'status': el.status.name}) %}
            {% endif %}
        {% endfor %}

        {% set run_started_at_str = run_started_at.strftime('%Y-%m-%d %H:%M:%S') %}

        {% set insert_query %}
            insert into {{ ref('re_data_test_history')}} (table_name, column_name, test_name, status, run_at) values

            {% for el in to_insert %}
                ( '{{ el.table_name }}' , {% if el.column_name %}'{{ el.column_name }}'{% else %}null{% endif %}, '{{ el.test_name }}', '{{ el.status }}', '{{ run_started_at_str }}' ) {% if not loop.last %}, {% endif %}
            {% endfor %}
        {% endset %}

        {% if to_insert %}
            {% do run_query(insert_query) %}
        {% endif %}

    {% endif %}

    {{ return ('') }}



{% endmacro %}

