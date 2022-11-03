
{%- macro bigquery__quote_string(str) %}
    r"""{{ str }}"""
{% endmacro %}