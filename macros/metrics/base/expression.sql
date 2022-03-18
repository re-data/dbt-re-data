{% macro metrics_base_expressions(table_name, time_filter, metrics, columns, table_level=False) %}

    {% set col_expr = [] %}

    {% for col in columns %}
        {% set column_name = re_data.row_value(col, 'column_name') %}
        {% do col_expr.extend(re_data.metrics_base_expression_column_all(table_name, metrics, col, time_filter)) %}
    {% endfor %}

    {% if table_level %}
        {% do col_expr.extend(re_data.metrics_base_expresion_table_all(table_name, time_filter, metrics)) %}
    {% endif %}

    {{ return (col_expr) }}

{% endmacro %}

{% macro metrics_base_expression_column_all(table_name, metrics, column, time_filter) %}

    {%- set col_expr = [] %}
    {%- set metrics_to_compute = [] %}
    {% set data_kind = re_data.get_column_type(column) %}
    {% set column_name = re_data.row_value(column, 'column_name') %}
    {% do metrics_to_compute.extend(var('re_data:metrics_base').get('column', {}).get(data_kind, [])) %}
    {% do metrics_to_compute.extend(metrics.get('column', {}).get(column_name, [])) %}    

    {% for metric_value in metrics_to_compute %}
        {% set metric_obj = re_data.extract_metric_config(metric_value) %}
        {% set expression = re_data.metrics_base_expression_column(column_name, metric_obj['metric'], metric_obj['config'], table_name, time_filter) %}
        {% do col_expr.append({ 'expr': expression, 'col_name': column_name, 'metric': metric_obj['metric']}) %}
    {% endfor %}

    {{ return (col_expr) }}

{% endmacro %}


{% macro metrics_base_expresion_table_all(table_name, time_filter, metrics) %}
    {%- set table_expr = [] %}
    {%- set metrics_to_compute = [] %}
    {% do metrics_to_compute.extend(var('re_data:metrics_base').get('table', [])) %}
    {% do metrics_to_compute.extend(metrics.get('table', [])) %}

    {% for metric_value in metrics_to_compute %}
        {% set metric_obj = re_data.extract_metric_config(metric_value) %}
        {% set expression = re_data.metrics_base_expression_table(time_filter, metric_obj['metric'], metric_obj['config'], table_name) %}
        {% do table_expr.append({ 'expr': expression, 'col_name': '', 'metric': metric_obj['metric']}) %}
    {% endfor %}

    {{ return (table_expr) }}

{% endmacro %}

{% macro metrics_base_expression_table(time_filter, metric_name, config, table_name) %}
    {% set metric_macro = re_data.get_metric_macro(metric_name) %}
    {% set context = {'time_filter': time_filter, 'metric_name': metric_name, 'config': config, 'table_name': table_name, 'column_name': none} %}

    {{ metric_macro(context) }}

{% endmacro %}


{%- macro metrics_base_expression_column(column_name, metric_name, config, table_name, time_filter) %}
    {% set metric_macro = re_data.get_metric_macro(metric_name) %}
    {% set context = {'time_filter': time_filter, 'metric_name': metric_name, 'config': config, 'table_name': table_name, 'column_name': re_data.quote_column_name(column_name)} %}

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

