
{% macro quote_text(text) %}
   {{ adapter.dispatch('quote_text', 're_data')(text) }}
{% endmacro %}

{% macro default__quote_text(text) %}
    {% set quoting = "$" + "$" %}
    {{quoting}}{{text}}{{quoting}}
{% endmacro %}

{% macro bigquery__quote_text(text) %}
    {% set quoting = '"""' %}
    {{quoting}}{{text}}{{quoting}}
{% endmacro %}
