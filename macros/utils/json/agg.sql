
{% macro agg_to_single_aray(json_string) %}
    {{ adapter.dispatch('agg_to_single_aray', 're_data')(json_string) }}
{% endmacro %}

{% macro default__agg_to_single_aray(json_string) %}
    '[' || string_agg(json_row, ',') || ']'
{% endmacro %}

{% macro redshift__agg_to_single_aray(json_string) %}
    '[' || listagg(json_row, ',') || ']'
{% endmacro %}

{% macro snowflake__agg_to_single_aray(json_string) %}
    '[' || listagg(json_row, ',') || ']'
{% endmacro %}