{#
# This file contains significant part of code derived from 
# https://github.com/fivetran/dbt_fivetran_utils/tree/v0.4.0 which is licensed under Apache License 2.0.
#}

{% macro percentile(percentile_field, partition_field, percent) -%}

{{ adapter.dispatch('percentile','re_data') (percentile_field, partition_field, percent) }}

{%- endmacro %}

--percentile calculation specific to Redshift
{% macro default__percentile(percentile_field, partition_field, percent)  %}

    percentile_cont( 
        {{ percent }} )
        within group ( order by {{ percentile_field }} )
        over ( partition by {{ partition_field }} )

{% endmacro %}

--percentile calculation specific to Redshift
{% macro redshift__percentile(percentile_field, partition_field, percent)  %}

    percentile_cont( 
        {{ percent }} )
        within group ( order by {{ percentile_field }} )
        over ( partition by {{ partition_field }} )

{% endmacro %}

--percentile calculation specific to BigQuery
{% macro bigquery__percentile(percentile_field, partition_field, percent)  %}

    percentile_cont( 
        {{ percentile_field }}, 
        {{ percent }}) 
        over (partition by {{ partition_field }}    
        )

{% endmacro %}

{% macro postgres__percentile(percentile_field, partition_field, percent)  %}

    percentile_cont( 
        {{ percent }} )
        within group ( order by {{ percentile_field }} )
    /* have to group by partition field */

{% endmacro %}

{% macro spark__percentile(percentile_field, partition_field, percent)  %}

    percentile( 
        {{ percentile_field }}, 
        {{ percent }}) 
        over (partition by {{ partition_field }}    
        )

{% endmacro %}
