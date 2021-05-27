{% macro timestamp_type() %}
    {{ adapter.dispatch('timestamp_type')() }}
{% endmacro %}

{% macro default__timestamp_type() %}
    timestamp without time zone
{% endmacro %}

{% macro bigquery__timestamp_type() %}
    TIMESTAMP
{% endmacro %}

{% macro snowflake__timestamp_type() %}
    TIMESTAMP_NTZ
{% endmacro %}

{% macro string_type() %}
    {{ adapter.dispatch('string_type')() }}
{% endmacro %}

{% macro default__string_type() %}
    text
{% endmacro %}

{% macro redshift__string_type() %}
    varchar(2047)
{% endmacro %}

{% macro bigquery__string_type() %}
    STRING
{% endmacro %}

{% macro snowflake__string_type() %}
    STRING
{% endmacro %}


{% macro boolean_type() %}
    {{ adapter.dispatch('boolean_type')() }}
{% endmacro %}

{% macro default__boolean_type() %}
    BOOLEAN
{% endmacro %}

{% macro bigquery__boolean_type() %}
    BOOLEAN
{% endmacro %}

{% macro snowflake__boolean_type() %}
    BOOLEAN
{% endmacro %}


{% macro numeric_type() %}
    {{ adapter.dispatch('numeric_type')() }}
{% endmacro %}

{% macro default__numeric_type() %}
    double precision
{% endmacro %}

{% macro bigquery__numeric_type() %}
    FLOAT64
{% endmacro %}

{% macro snowflake__numeric_type() %}
    FLOAT
{% endmacro %}



