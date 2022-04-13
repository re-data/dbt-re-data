
{% macro row_value(agate_row, column) %}
    {{ return (agate_row[re_data.name_in_db(column)]) }}
{% endmacro %}

{% macro agate_to_list(table) %}
    {% set col_names = table.column_names %}
    {% set query_result = [] %}
    {% for row in table.rows %}
        {% do query_result.append('' ~ row.dict()) %}
    {% endfor %}
    {{ return (query_result) }}
{% endmacro %}