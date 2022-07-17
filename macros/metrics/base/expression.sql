{% macro metrics_base_expressions(model, columns, table_level=False) %}

    {% set col_expr = [] %}

    {% for col in columns %}
        {% set column_name = re_data.row_value(col, 'column_name') %}
        {% do col_expr.extend(re_data.metrics_base_expression_column_all(model, col)) %}
    {% endfor %}

    {% if table_level %}
        {% do col_expr.extend(re_data.metrics_base_expresion_table_all(model)) %}
    {% endif %}

    {{ return (col_expr) }}

{% endmacro %}

{% macro metrics_base_expression_column_all(model, column) %}

    {%- set col_expr = [] %}
    {%- set metrics_to_compute = [] %}
    {% set column_name = re_data.row_value(column, 'column_name') %}
    {% set data_type = model.columns_info[column_name].data_type %}
    {% do metrics_to_compute.extend(model.metrics.get('group').get('column', {}).get(data_type, [])) %}
    {% do metrics_to_compute.extend(model.metrics.get('additional').get('column', {}).get(column_name, [])) %} 

    {% for metric_value in metrics_to_compute %}
        {% set metric_obj = re_data.extract_metric_config(metric_value) %}
        {% set expression = re_data.metrics_base_expression_column(model, column_name, metric_obj['metric'], metric_obj['config']) %}
        {% do col_expr.append({ 'expr': expression, 'col_name': column_name, 'metric': metric_obj['metric']}) %}
    {% endfor %}

    {{ return (col_expr) }}

{% endmacro %}


{% macro metrics_base_expresion_table_all(model) %}
    {%- set table_expr = [] %}
    {%- set metrics_to_compute = [] %}
    {% do metrics_to_compute.extend(model.metrics.get('group').get('table', [])) %}
    {% do metrics_to_compute.extend(model.metrics.get('additional').get('table', [])) %}

    {% for metric_value in metrics_to_compute %}
        {% set metric_obj = re_data.extract_metric_config(metric_value) %}
        {% set expression = re_data.metrics_base_expression_table(model, metric_obj['metric'], metric_obj['config']) %}
        {% do table_expr.append({ 'expr': expression, 'col_name': '', 'metric': metric_obj['metric']}) %}
    {% endfor %}

    {{ return (table_expr) }}

{% endmacro %}

{% macro metrics_base_expression_table(model, metric_name, config) %}
    {% set metric_macro = re_data.get_metric_macro(metric_name) %}
    {% set context = {'time_filter': model.time_filter, 'metric_name': metric_name, 'config': config, 'table_name': model.table_name, 'column_name': none} %}

    {{ metric_macro(context) }}

{% endmacro %}


{%- macro metrics_base_expression_column(model, column_name, metric_name, config) %}
    {% set metric_macro = re_data.get_metric_macro(metric_name) %}
    {% set context = {'time_filter': model.time_filter, 'metric_name': metric_name, 'config': config, 'table_name': model.table_name, 'column_name': re_data.quote_column_name(column_name)} %}

    {{ metric_macro(context) }}

{% endmacro %}

{% macro extract_metric_config(metric_value) %}

    {% set config = none %}

    {% if metric_value is mapping %}
        {% set metric = metric_value.keys() | first %}
        {% if metric_value[metric] is none %}
            {{ exceptions.raise_compiler_error("Empty configuration passed for metric: " ~ metric ~ ". If the metric doesn't use a config, please use the column name as a string.") }}
        {% endif %}

        {% set config = metric_value[metric] %}
    {%- else %}
        {% set metric = metric_value %}
    {% endif %}

    {{ return ({'metric': metric, 'config': config}) }}

{% endmacro %}

{%- macro get_metric_macro(metric_name) %}
    {% set macro_name = 're_data_metric' + '_' + metric_name %}

    {% if context['re_data'].get(macro_name) %}
        {% set metric_macro = context['re_data'][macro_name] %}
    {%- else %}
        {% set metric_macro = context[project_name][macro_name] %}
    {% endif %}

    {{ return (metric_macro) }}

{% endmacro %}

