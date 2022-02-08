
{% macro bool_to_string(column) %}
    (
    case when {{ column }} = true then 'true'
         when {{ column }} = false then 'false'
    end
    ) as {{ column }}
{% endmacro %}