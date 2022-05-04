
{%- macro quote_string(str) %}
    {{ adapter.dispatch('quote_string', 're_data')(str) }}
{% endmacro %}

{%- macro default__quote_string(str) %}
    $${{ str }}$$
{% endmacro %}


{%- macro quote_new_line() %}{{ adapter.dispatch('quote_new_line', 're_data')() }}{% endmacro %}

{%- macro default__quote_new_line() %}'\134\134n'{% endmacro %}