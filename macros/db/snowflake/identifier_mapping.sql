
{% macro snowflake__name_in_db(name) %}
    {% if name %}
        {{ return (name.upper()) }}
    {% else %}
        {{ return (name) }}
    {% endif %}
{% endmacro %}