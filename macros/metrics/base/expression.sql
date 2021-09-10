{% macro metrics_base_expressions(table_name, time_filter, metrics, columns, table_level=False) %}

    {% set col_expr = [] %}

    {% for col in columns %}
        {% set column_name = re_data.row_value(col, 'column_name') %}
        {% do col_expr.extend(re_data.metrics_base_expression_column_all(table_name, metrics, col)) %}
    {% endfor %}

    {% if table_level %}
        {% do col_expr.extend(re_data.metrics_base_expresion_table_all(table_name, time_filter, metrics)) %}
    {% endif %}

    {{ return (col_expr) }}

{% endmacro %}

{% macro metrics_base_expression_column_all(table_name, metrics, column) %}

    {%- set col_expr = [] %}
    {%- set metrics_to_compute = [] %}
    {% set data_kind = re_data.get_column_type(column) %}
    {% set column_name = re_data.row_value(column, 'column_name') %}
    {% do metrics_to_compute.extend(var('re_data:metrics_base')['column'].get(data_kind, [])) %}
    {% do metrics_to_compute.extend(metrics.get('column', {}).get(column_name, [])) %}    

    {% for metric in metrics_to_compute %}
        {% set expression = re_data.metrics_base_expression_column(column_name, metric) %}
        {% do col_expr.append({ 'expr': expression, 'col_name': column_name, 'metric': metric}) %}
    {% endfor %}

    {{ return (col_expr) }}

{% endmacro %}


{% macro metrics_base_expresion_table_all(table_name, time_filter, metrics) %}
    {%- set table_expr = [] %}
    {%- set metrics_to_compute = [] %}
    {% do metrics_to_compute.extend(var('re_data:metrics_base')['table']) %}
    {% do metrics_to_compute.extend(metrics.get('table', [])) %}

    {% for metric in metrics_to_compute %}
        {% set expression = re_data.metrics_base_expression_table(time_filter, metric) %}
        {% do table_expr.append({ 'expr': expression, 'col_name': '', 'metric': metric}) %}
    {% endfor %}

    {{ return (table_expr) }}

{% endmacro %}

{% macro metrics_base_expression_table(time_filter, metric) %}
    {% set macro_name = 'metric' + '_' + metric %}

    {% if context['re_data'].get(macro_name) %}
        {{ context['re_data'][macro_name](time_filter) }}
    {%- else %}
        {{ context[project_name][macro_name](time_filter) }}
    {% endif %}

{% endmacro %}


{%- macro metrics_base_expression_column(column_name, metric) %}
    {% set macro_name = 'metric' + '_' + metric %}

    {% if context['re_data'].get(macro_name) %}
        {{ context['re_data'][macro_name](column_name) }}
    {%- else %}
        {{ context[project_name][macro_name](column_name) }}
    {% endif %}

{% endmacro %}


