
{% macro to_empty_helper(column) %}
    '"' ||
    replace (
        coalesce(
            cast ( {{column}} as {{ string_type()}}),
            ''
        ),
        '"', '\"'
    ) || '"'

{% endmacro %}

{% macro row_to_json(model) %}
    {%- set columns = adapter.get_columns_in_relation(model) -%}
    '{' ||
    {%- for column in columns %}
        '"{{ column.column }}": ' ||
        {{ to_empty_helper(column.column) }} 
        {%- if not loop.last %} || ',' || {%- endif %}
    {%- endfor %}
    || '}'

{% endmacro %}