
{% macro metric_expression(model, table, metric, expression, column_name=None, condition=None) %}
    select * from {{ref('re_data_base_metrics')}}
    where
        table_name = '{{ re_data.full_table_name_values(table.identifier, table.schema, table.database)}}' and
        metric = '{{ metric }}' and
        {% if condition is not none %}
            {{ condition }} and
        {% endif %}
        {% if column_name is none %}
        not ( {{ expression }} )
        {% else %}
        column_name = '{{ column_name }}' and
        not ( {{ expression }} )
        {% endif %}

{% endmacro %}


{% test metric_expression_is_true(model, table, metric, expression, column_name=None, condition=None) %}
    {{ re_data.metric_expression(model, table, metric, expression, column_name=None, condition=None) }}
{% endtest %}


{% test metric_equal_to(model, table, metric, value, column_name=None, condition=None) %}
    {{ re_data.metric_expression(model, table, metric, 'value = ' ~ value, column_name, condition) }}
{% endtest %}


{% test metric_in_range(model, table, metric, min_value, max_value, column_name=None, condition=None) %}
    {{ re_data.metric_expression(model, table, metric, 'value >= ' ~ min_value ~ ' and value <= ' ~ max_value, column_name, condition) }}
{% endtest %}