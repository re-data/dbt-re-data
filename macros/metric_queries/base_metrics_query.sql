{% macro base_metrics_query(mtable) %}

    {% set table_name = row_value(mtable, 'table_name') %}

    {% set columns_query %}
        select * from {{ ref('re_data_columns') }}
        where table_name = '{{ table_name }}'
    {% endset %}

    {% set columns = run_query(columns_query) %}

    {% for col in columns %}
        {{ metrics_for_column(mtable, col) }}
    {% endfor %}

    {{  metrics_for_whole_table(mtable) }}

{% endmacro %}


{% macro metrics_for_column(mtable, column) %}

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

    {% for func in column_metrics[data_kind] %}
        {% set column_name = row_value(column, 'column_name') %}
        {% set select_as = column_name + '___' + func %}
        {{ column_expression(column_name, func) }} as {{ quote_col(select_as) }},
    {% endfor %}

{% endmacro %}


{% macro metrics_for_whole_table(mtable, column) %}
    count(*) as {{ quote_col('___row_count') }}
{% endmacro %}