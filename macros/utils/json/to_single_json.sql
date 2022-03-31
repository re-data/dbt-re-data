
{% macro to_json_string_value_or_null(column) %}
    (
        case 
            when {{column}} is null then 'null'
            else '"' || regexp_replace(replace(cast({{column}} as {{string_type()}}), '"', '\"'), '\n', '\\n', 'g')  || '"'
        end
    )
{% endmacro %}

{% macro to_single_json(columns) %}
    '{' ||
    {%- for column in columns %}
        '"{{ column }}": ' ||
        {{ to_json_string_value_or_null(column) }} 
        {%- if not loop.last %} || ',' || {%- endif %}
    {%- endfor %}
    || '}'

{% endmacro %}