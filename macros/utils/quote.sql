

{% macro quote_column(col_name) %}
    {{ adapter.dispatch('quote_column', 're_data')(col_name) }}
{% endmacro %}

{% macro default__quote_column(col_name) %}
    "{{ col_name }}"
{% endmacro %}

{% macro bigquery__quote_column(col_name) %}
    `{{ col_name }}`
{% endmacro %}