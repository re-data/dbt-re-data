
{% macro quote_constant(value) %}
    {{ return (value.replace("'", "''")) }}
{% endmacro %}

