{% macro json_extract(string, string_path) -%}

{{ adapter.dispatch('json_extract', 're_data') (string, string_path) }}

{%- endmacro %}

-- use fivetran_utils by default
{% macro default__json_extract(string, string_path) %}

  fivetran_utils.json_extract(string, string_path)

{% endmacro %}
