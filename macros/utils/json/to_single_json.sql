{% macro to_json_string_value_or_null(column) %}
    (
        case 
            when {{column}} is null then 'null'
            else '"' || replace(replace(cast({{column}} as {{string_type()}}), '"', {{escape_seq_for_json('"') }}), {{ re_data.quote_string("\\'") }}, {{ re_data.quote_string("'")}} ) || '"'
        end
    )
{% endmacro %}

{% macro to_single_json(columns) %}
    '{' ||
    {%- for column in columns %}
        '"{{ column }}": ' ||
        {{ re_data.clean_blacklist(to_json_string_value_or_null(column), ['\n'], ' ') }} 
        {%- if not loop.last %} || ',' || {%- endif %}
    {%- endfor %}
    || '}'
{% endmacro %}