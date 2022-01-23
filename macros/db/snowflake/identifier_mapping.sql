
{% macro snowflake__name_in_db(name) %}
    {{ return (name.upper()) }}
{% endmacro %}