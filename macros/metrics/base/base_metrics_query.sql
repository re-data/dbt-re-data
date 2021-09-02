{% macro get_insert_metrics_query(table_name, time_filter, ref_model, columns, table_level=False) %}

    {% set col_exprs = base_metrics_query(table_name, columns, table_level) %}
    {% if col_exprs == [] %}
        {{ return ('') }}
    {% endif %}

    insert into {{ ref(ref_model) }}
    with temp_table_metrics as (
    select 
        {%- for col_expr in col_exprs %}
            {{ col_expr.expr }} as {{ col_expr.col_name + '___' + col_expr.metric }}
            {%- if not loop.last %},{%- endif %}
        {% endfor %}
    from 
        {{ table_name }}
    where
        {{ in_time_window(time_filter) }}
    )

    {%- for col_expr in col_exprs %}
        select '{{table_name}}' as table_name, '{{ col_expr.col_name }}' as column_name, '{{ col_expr.metric }}' as metric, {{ col_expr.col_name + '___' + col_expr.metric }} as value
        from temp_table_metrics
        {% if not loop.last %}union all{% endif %}
    {% endfor %}

{% endmacro %}
    

{% macro base_metrics_query(table_name, columns, table_level=False) %}

    {% set col_expr = [] %}

    {% for col in columns %}
        {% do col_expr.extend(metrics_for_column(col)) %}
    {% endfor %}

    {% if table_level %}
        {% do col_expr.extend(metrics_for_whole_table()) %}
    {% endif %}

    {{ return (col_expr) }}

{% endmacro %}


{% macro metrics_for_column(column) %}

{%- set col_expr = [] %}

{% set column_metrics_yml -%}
numeric:
    - min
    - max
    - avg
    - stddev
    - variance
    - count_nulls
text:
    - min_length
    - max_length
    - avg_length
    - count_nulls
    - count_missing
{%- endset %}

    {% set column_metrics = fromyaml(column_metrics_yml) %}
    {% set data_kind = get_column_type(column) %}

    {% for metric in column_metrics[data_kind] %}
        {% set column_name = row_value(column, 'column_name') %}
        {% set expression = column_expression(column_name, metric) %}
        {% do col_expr.append({ 'expr': expression, 'col_name': column_name, 'metric': metric}) %}
    {% endfor %}

    {{ return (col_expr) }}

{% endmacro %}


{% macro metrics_for_whole_table() %}
    {{ return ([{'expr': 'count(1)', 'col_name': '', 'metric': 'row_count' }]) }}
{% endmacro %}