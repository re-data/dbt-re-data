{{
    config(
        materialized='incremental',
        unique_key = 'generated_at'
    )
}}

-- depends_on: {{ ref('re_data_alerting') }}
-- depends_on: {{ ref('re_data_base_metrics') }}
-- depends_on: {{ ref('re_data_schema_changes') }}

{% if execute %}
    with json_metrics
        as ( select {{ (row_to_json(ref('re_data_base_metrics'))) }} as json_row
           from {{ ref('re_data_base_metrics') }} )
    
    select {{ agg_to_single_aray('json_row') }} as json_overview from json_metrics
    
    
{% else %}
    {{ re_data.empty_overview_table() }}
{% endif %}