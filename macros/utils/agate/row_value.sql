
{% macro row_value(agate_row, column) %}
    {{ return (agate_row[re_data.name_in_db(column)]) }}
{% endmacro %}
