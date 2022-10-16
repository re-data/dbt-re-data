{#
# This file contains significant part of code derived from 
# https://github.com/fivetran/dbt_fivetran_utils/tree/v0.4.0 which is licensed under Apache License 2.0.
#}

{% macro json_extract(string, string_path) -%}

{{ adapter.dispatch('json_extract') (string, string_path) }}

{%- endmacro %}

{% macro default__json_extract(string, string_path) %}

  json_extract_path_text({{string}}, {{ "'" ~ string_path ~ "'" }} )
 
{% endmacro %}

{% macro snowflake__json_extract(string, string_path) %}

  json_extract_path_text(try_parse_json( {{string}} ), {{ "'" ~ string_path ~ "'" }} )

{% endmacro %}

{% macro redshift__json_extract(string, string_path) %}

  case when is_valid_json( {{string}} ) then json_extract_path_text({{string}}, {{ "'" ~ string_path ~ "'" }} ) else null end
 
{% endmacro %}

{% macro bigquery__json_extract(string, string_path) %}

  json_extract_scalar({{string}}, {{ "'$." ~ string_path ~ "'" }} )

{% endmacro %}

{% macro postgres__json_extract(string, string_path) %}

  {{string}}::json->>{{"'" ~ string_path ~ "'" }}

{% endmacro %}