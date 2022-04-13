{% macro timestamp_type() %}
    {{ adapter.dispatch('timestamp_type', 're_data')() }}
{% endmacro %}

{% macro default__timestamp_type() %}
    timestamp without time zone
{% endmacro %}

{% macro redshift__timestamp_type() %}
    TIMESTAMP
{% endmacro %}

{% macro bigquery__timestamp_type() %}
    TIMESTAMP
{% endmacro %}

{% macro snowflake__timestamp_type() %}
    TIMESTAMP_NTZ
{% endmacro %}

{% macro string_type() %}
    {{ adapter.dispatch('string_type', 're_data')() }}
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

{% macro long_string_type() %}
    {{ adapter.dispatch('long_string_type', 're_data')() }}
{% endmacro %}

{% macro default__long_string_type() %}
    {{ re_data.string_type() }}
{% endmacro %}

{% macro redshift__long_string_type() %}
    varchar(65535)
{% endmacro %}

{% macro integer_type() %}
    INTEGER
{% endmacro %}


{% macro boolean_type() %}
    {{ adapter.dispatch('boolean_type', 're_data')() }}
{% endmacro %}

{% macro default__boolean_type() %}
    BOOLEAN
{% endmacro %}

{% macro redshift__boolean_type() %}
    boolean
{% endmacro %}

{% macro bigquery__boolean_type() %}
    BOOLEAN
{% endmacro %}

{% macro snowflake__boolean_type() %}
    BOOLEAN
{% endmacro %}


{% macro numeric_type() %}
    {{ adapter.dispatch('numeric_type', 're_data')() }}
{% endmacro %}

{% macro default__numeric_type() %}
    double precision
{% endmacro %}

{% macro redshift__numeric_type() %}
    DOUBLE PRECISION
{% endmacro %}

{% macro bigquery__numeric_type() %}
    FLOAT64
{% endmacro %}

{% macro snowflake__numeric_type() %}
    FLOAT
{% endmacro %}



