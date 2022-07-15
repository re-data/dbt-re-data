{% macro percentile(percentile_field, partition_field, percent) -%}

{{ adapter.dispatch('percentile_impl', 're_data') (percentile_field, partition_field, percent) }}

{%- endmacro %}

-- use fivetran_utils by default
{% macro default__percentile_impl(percentile_field, partition_field, percent)  %}
    {{ fivetran_utils.percentile(percentile_field, partition_field, percent) }}
{% endmacro %}

