
{% macro row_value(agate_row, column) %}
    {{ return (agate_row[re_data.name_in_db(column)]) }}
{% endmacro %}

{% macro agate_to_list(table) %}
    {% set col_names = table.column_names %}
    {% set query_result = [] %}
    {% for row in table.rows %}
        {% set pairs = [] %}
        {% for col_name in col_names %}
            {% set value = row.get(col_name) | string %}
            {% do pairs.append('"' ~ (col_name | lower) ~ '":' ~ '"' ~ (value | replace('"', '\\\"') ) ~ '"') %}
        {% endfor %}
        {% set joined_pairs = '{' ~ (pairs | join(',')) ~ '}' %}
        {% do query_result.append(joined_pairs) %}
    {% endfor %}
    {% set query_result = '[' ~ (query_result | join(',')) ~ ']' %}
    {{ return (query_result) }}
{% endmacro %}