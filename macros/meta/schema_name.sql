
{% macro schema_name(name) %}
    {% set result = adapter.dispatch('schema_name', 're_data')(name) %}
    {{ return(result) }}
{% endmacro %}

{% macro default__schema_name(name) %}
    {{ return (name) }}
{% endmacro %}

{% macro snowflake__schema_name(name) %}
    {{ return (name.upper()) }}
{% endmacro %}