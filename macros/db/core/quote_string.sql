
{%- macro quote_string(str) %}
    {{ adapter.dispatch('quote_string', 're_data')(str) }}
{% endmacro %}

{%- macro default__quote_string(str) %}
    $${{ str }}$$
{% endmacro %}

