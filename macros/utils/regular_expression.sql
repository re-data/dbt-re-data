{% macro regex_match_expression(column_name, pattern) %}
    {{ adapter.dispatch('regex_match_expression', 're_data')(column_name, pattern) }}
{% endmacro %}

{% macro default__regex_match_expression(column_name, pattern) %}
    ({{column_name}} ~ '{{pattern}}')
{% endmacro %}

{% macro bigquery__regex_match_expression(column_name, pattern) %}
    regexp_contains({{column_name}}, r'{{pattern}}')
{% endmacro %}

{% macro snowflake__regex_match_expression(column_name, pattern) %}
    regexp_like({{column_name | upper}}, '{{pattern}}')
{% endmacro %}
