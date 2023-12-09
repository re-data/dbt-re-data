
{% macro bool_to_string(column) %}
    (
    case when cast({{ column }} as boolean) = true then 'true'
         when cast({{ column }} as boolean) = false then 'false'
    end
    ) as {{ column }}
{% endmacro %}
