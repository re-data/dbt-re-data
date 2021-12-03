
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

{% macro to_json_string_value_or_null(column) %}
    (
        case 
            when {{column}} is null then 'null'
            else '"' || replace(cast({{column}} as {{string_type()}}), '"', '\"')  || '"'
        end
    )
{% endmacro %}

{% macro row_to_json(model) %}
    {%- set columns = adapter.get_columns_in_relation(model) -%}
    '{' ||
    {%- for column in columns %}
        '"{{ column.column }}": ' ||
        {{ to_json_string_value_or_null(column.column) }} 
        {%- if not loop.last %} || ',' || {%- endif %}
    {%- endfor %}
    || '}'

{% endmacro %}