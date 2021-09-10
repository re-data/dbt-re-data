
{% test metric_expression_is_true(model, table, column_name, metric, expression) %}
    select * from {{ref('re_data_base_metrics')}}
    where
        metric = {{ metric }} and
        {% if column_name is none %}
        not ( {{ expression }} )
        {%- else %}
        column_name = {{ column_name }} and
        not ( {{ expression }} )
        {%- endif %}

{% endtest %}