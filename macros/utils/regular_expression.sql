{% macro regexp_match_expression(column_name, pattern) %}
    {{ adapter.dispatch('regexp_match_expression', 're_data')(column_name, pattern) }}
{% endmacro %}

{% macro default__regexp_match_expression(column_name, pattern) %}
    {{column_name}} ~ '{{pattern}}'
{% endmacro %}

{% macro bigquery__regexp_match_expression(column_name, pattern) %}
    regexp_contains({{column_name}}, '{{pattern}}')
{% endmacro %}

{% macro snowflake__regexp_match_expression(column_name, pattern) %}
    regexp_like({{column_name | upper}}, '{{pattern}}')
{% endmacro %}
