{% macro is_list(obj) %}
    {% if not obj %}
        {{ return (False) }}
    {% endif %}
    {% set check = obj is iterable and (obj is not string and obj is not mapping) %}
    {{ return (check) }}
{% endmacro %}