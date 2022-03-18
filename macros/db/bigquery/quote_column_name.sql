{% macro bigquery__quote_column_name(column_name) %}
    {% set quoted_col_name = '`' + column_name + '`' %}
    {{ return(quoted_col_name) }}
{% endmacro %}