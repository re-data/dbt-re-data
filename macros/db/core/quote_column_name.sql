{% macro quote_column_name(column_name) %}
    {% set col_name = adapter.dispatch('quote_column_name', 're_data')(column_name) %}
    {{ return(col_name) }}
{% endmacro %}


{% macro default__quote_column_name(column_name) %}
    {% set quoted_col_name = '"' + column_name + '"' %}
    {{ return(quoted_col_name) }}
{% endmacro %}