{%- macro trino__quote_string(str) %}
    '{{ str }}'
{% endmacro %}

{%- macro trino__quote_new_line() %}'\n'{% endmacro %}

