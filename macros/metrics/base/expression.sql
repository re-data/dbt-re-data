{% macro metrics_base_expressions(table_name, time_filter, columns, table_level=False) %}

    {% set col_expr = [] %}

    {% for col in columns %}
        {% do col_expr.extend(re_data.metrics_base_expression_column_all(table_name, col)) %}
    {% endfor %}

    {% if table_level %}
        {% do col_expr.extend(re_data.metrics_base_expresion_table_all(table_name, time_filter)) %}
    {% endif %}

    {{ return (col_expr) }}

{% endmacro %}

{% macro metrics_base_expression_column_all(table_name, column) %}

    {%- set col_expr = [] %}
    {% set column_metrics = var('re_data:metrics_base')['column'] %}
    {% set data_kind = re_data.get_column_type(column) %}

    {% for metric in column_metrics[data_kind] %}
        {% set column_name = re_data.row_value(column, 'column_name') %}
        {% set expression = re_data.metrics_base_expression_column(column_name, metric) %}
        {% do col_expr.append({ 'expr': expression, 'col_name': column_name, 'metric': metric}) %}
    {% endfor %}

    {{ return (col_expr) }}

{% endmacro %}


{% macro metrics_base_expresion_table_all(table_name, time_filter) %}
    {%- set table_expr = [] %}
    {% set table_metrics = var('re_data:metrics_base')['table'] %}

    {% for metric in table_metrics %}
        {% set expression = re_data.metrics_base_expression_table(time_filter, metric) %}
        {% do table_expr.append({ 'expr': expression, 'col_name': '', 'metric': metric}) %}
    {% endfor %}

    {{ return (table_expr) }}

{% endmacro %}

{% macro metrics_base_expression_table(time_filter, metric) %}

    {%- if metric == 'row_count' %}
        count(1)

    {%- elif metric == 'freshness' %}
        {{ freshness_expression(time_filter) }}
    
    {%- else %}
        {{ context[project_name][metric](time_filter) }}

    {% endif %}

{% endmacro %}


{%- macro metrics_base_expression_column(column_name, metric) %}

    {%- if metric == 'max' %}
        max({{column_name}})

    {%- elif metric == 'min' %}
        min({{column_name}})

    {%- elif metric == 'avg' %}
        avg(cast ({{column_name}} as {{ numeric_type() }}))

    {%- elif metric == 'stddev' %}
        stddev(cast ( {{column_name}} as {{ numeric_type() }}))

    {%- elif metric == 'variance' %}
        variance(cast ( {{column_name}} as {{ numeric_type() }}))

    {%- elif metric == 'max_length' %}
        max(length({{column_name}}))

    {%- elif metric == 'min_length' %}
        min(length({{column_name}}))

    {%- elif metric == 'avg_length' %}
        avg(cast (length( {{column_name}} ) as {{ numeric_type() }}))

    {%- elif metric == 'count_nulls' %}
        coalesce(
            sum(
                case when {{column_name}} is null
                    then 1
                else 0
                end
            ), 0
        )

    {%- elif metric == 'count_missing' %}
        coalesce(
            sum(
                case 
                when {{column_name}} is null
                    then 1
                when {{column_name}} = ''
                    then 1
                else 0
                end
            ), 0
        )
    {%- else %}
        {{ context[project_name][metric](column_name) }}
        
    {% endif %}

{% endmacro %}


