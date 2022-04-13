
{%- macro bigquery__quote_string(str) %}
    """{{ str }}"""
{% endmacro %}