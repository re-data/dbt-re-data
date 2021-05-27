{% macro quote_col(some_name) %}
    {{ adapter.dispatch('quote_col')(some_name) }}
{% endmacro %}

{% macro default__quote_col(some_name) %}
    "{{some_name}}"
{% endmacro %}

{% macro bigquery__quote_col(some_name) %}
    `{{some_name}}`
{% endmacro %}