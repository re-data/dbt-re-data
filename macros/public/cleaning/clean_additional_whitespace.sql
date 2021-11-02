{% macro clean_additional_whitespaces(column_name) %}
    {{ adapter.dispatch('clean_additional_whitespaces', 're_data')(column_name) }}
{% endmacro %}

{% macro default__clean_additional_whitespaces(column_name) %}
    trim(regexp_replace( {{ column_name }}, '\s\s+', ' '))
{% endmacro %}

{% macro postgres__clean_additional_whitespaces(column_name) %}
    trim(regexp_replace( {{ column_name }}, '\s\s+', ' ', 'g'))
{% endmacro %}

{% macro redshift__clean_additional_whitespaces(column_name) %}
    trim(regexp_replace( {{ column_name }}, '\\s\\s+', ' '))
{% endmacro %}

{% macro bigquery__clean_additional_whitespaces(column_name) %}
    trim(regexp_replace( {{ column_name }}, r'\s\s+', ' '))
{% endmacro %}

{% macro snowflake__clean_additional_whitespaces(column_name) %}
    trim(regexp_replace( {{ column_name }}, '\\s\\s+', ' '))
{% endmacro %}