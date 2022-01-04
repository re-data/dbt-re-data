
{% macro save_results_history(results) %}
    -- depends_on: {{ ref('re_data_test_history') }}

    {% if execute and results %}
        {% set to_insert = [] %}
        {% for el in results %}
            {% if el.node.resource_type.name == 'Test' %}
                {% do to_insert.append({ 'table_name': el.node.file_key_name, 'column_name': el.node.column_name , 'test_name': el.node.name, 'status': el.status.name}) %}
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

