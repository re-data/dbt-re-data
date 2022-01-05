
{% macro save_test_history(results) %}
    -- depends_on: {{ ref('re_data_test_history') }}

    {% set re = modules.re %}

    {% if execute and results %}
        {% set to_insert = [] %}
        {% for el in results %}
            {% if el.node.resource_type.name == 'Test' %}
                {% set any_refs = re.findall("ref\(\'(?P<name>.*)\'\)", el.node.test_metadata.kwargs['model']) %}
                {% set any_source = re.findall("source\(\'(?P<one>.*)\'\,\s+\'(?P<two>.*)\'\)", el.node.test_metadata.kwargs['model']) %}
                {% set schema = el.node.schema.replace('_dbt_test__audit', '') %}
                {% set database = el.node.database %}
                {% if any_refs %}
                    {% set model_name = database + '.' + schema + '.' + any_refs[0] %} 
                {% elif any_source %}
                    {% set model_name = database + '.' + any_source[0][0] + '.' + any_source[0][1] %} 
                {% else %}
                    {% set model_name = none %}
                {% endif %}

                {% do to_insert.append({ 'table_name': model_name, 'column_name': el.node.column_name , 'test_name': el.node.name, 'status': el.status.name}) %}
            {% endif %}
        {% endfor %}

        {% set run_started_at_str = run_started_at.strftime('%Y-%m-%d %H:%M:%S') %}

        {% set insert_query %}
            insert into {{ ref('re_data_test_history')}} (table_name, column_name, test_name, status, runned_at) values

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

