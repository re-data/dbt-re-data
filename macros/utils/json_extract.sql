{% macro json_extract(string, string_path) -%}

{{ adapter.dispatch('json_extract', 're_data') (string, string_path) }}

{%- endmacro %}


{% macro default__json_extract(string, string_path) %}

  fivetran_utils.json_extract(string, string_path)

{% endmacro %}


{% macro trino__json_extract(string, string_path) %}

  json_extract_scalar({{string}}, {{ "'$." ~ string_path ~ "'" }} )

{% endmacro %}
