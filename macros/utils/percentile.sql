{% macro percentile(percentile_field, partition_field, percent) -%}

{{ adapter.dispatch('percentile', 're_data') (percentile_field, partition_field, percent) }}

{%- endmacro %}

-- use fivetran_utils
{% macro default__percentile(percentile_field, partition_field, percent)  %}
    fivetran_utils.percentile(percentile_field, partition_field, percent)
{% endmacro %}

--percentile calculation specific to Trino
{% macro trino__percentile(percentile_field, partition_field, percent)  %}
    approx_percentile(
        {{ percentile_field }},
        {{ percent }})
        over (partition by {{ partition_field }}
        )

{% endmacro %}

