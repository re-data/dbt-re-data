{% macro metrics_base_expressions(table_name, columns, table_level=False) %}

    {% set col_expr = [] %}

    {% for col in columns %}
        {% do col_expr.extend(re_data.metrics_base_col_expr(table_name, col)) %}
    {% endfor %}

    {% if table_level %}
        {% do col_expr.extend(re_data.metrics_base_table_expr(table_name)) %}
    {% endif %}

    {{ return (col_expr) }}

{% endmacro %}

{% macro metrics_base_col_expr(table_name, column) %}

    {%- set col_expr = [] %}
    {% set column_metrics = var('re_data:metrics_base') %}
    {% set data_kind = re_data.get_column_type(column) %}

    {% for metric in column_metrics[data_kind] %}
        {% set column_name = row_value(column, 'column_name') %}
        {% set expression = re_data.metrics_base_expression_specific(column_name, metric) %}
        {% do col_expr.append({ 'expr': expression, 'col_name': column_name, 'metric': metric}) %}
    {% endfor %}

    {{ return (col_expr) }}

{% endmacro %}


{% macro metrics_base_table_expr(table) %}
    {{ return ([{'expr': 'count(1)', 'col_name': '', 'metric': 'row_count' }]) }}
{% endmacro %}


{%- macro metrics_base_expression_specific(column_name, func) %}

    {%- if func == 'max' %}
        max({{column_name}})

    {%- elif func == 'min' %}
        min({{column_name}})

    {%- elif func == 'avg' %}
        avg(cast ({{column_name}} as {{ numeric_type() }}))

    {%- elif func == 'stddev' %}
        stddev(cast ( {{column_name}} as {{ numeric_type() }}))

    {%- elif func == 'variance' %}
        variance(cast ( {{column_name}} as {{ numeric_type() }}))

    {%- elif func == 'max_length' %}
        max(length({{column_name}}))

    {%- elif func == 'min_length' %}
        min(length({{column_name}}))

    {%- elif func == 'avg_length' %}
        avg(cast (length( {{column_name}} ) as {{ numeric_type() }}))

    {%- elif func == 'count_nulls' %}
        coalesce(
            sum(
                case when {{column_name}} is null
                    then 1
                else 0
                end
            ), 0
        )

    {%- elif func == 'count_missing' %}
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
        {{ log('[re_data_log] - fatal unrecognized metric ' ~ func, True) }}

    {% endif %}

{% endmacro %}


