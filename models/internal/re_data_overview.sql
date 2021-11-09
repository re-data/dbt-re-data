{{
    config(
        materialized='incremental',
        unique_key = 'generated_at'
    )
}}

-- depends_on: {{ ref('re_data_z_score') }}
-- depends_on: {{ ref('re_data_base_metrics') }}

{% if execute %}
    {% set overview_query %}
        with z_score_cte as (
            select * from {{ ref('re_data_z_score') }}
        ),
        base_metrics_cte as (
            select * from {{ ref('re_data_base_metrics') }}
        )
        select 'anomalies' as component_name, json_agg(row_to_json(z)) as component_json, {{ dbt_utils.current_timestamp_in_utc() }} as generated_at
        from z_score_cte z
        union all
        select 'metrics' as component_name, json_agg(row_to_json(b)) as component_json, {{ dbt_utils.current_timestamp_in_utc() }} as generated_at
        from base_metrics_cte b
        
    {% endset %}

    {% set overview_result = run_query(overview_query) %}
    {% do overview_result.to_json('target/re_data_overview.json') %}

    {{ overview_query }}

{% else %}
    {{ re_data.empty_overview_table() }}
{% endif %}