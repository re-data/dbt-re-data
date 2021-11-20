{{
    config(
        materialized='incremental',
        unique_key = 'generated_at'
    )
}}

-- depends_on: {{ ref('re_data_alerting') }}
-- depends_on: {{ ref('re_data_base_metrics') }}

{% if execute %}
    {% set dbt_graph = tojson(graph) %}
    {# This ensures that the concatenated dollar sign is escaped in jinja as it won't compile if written as a literal #}
    {% set two_dollar_sign = "$" + "$" %}
    {% set overview_query %}
        with z_score_cte as (
            select * from {{ ref('re_data_alerting') }}
        ),
        base_metrics_cte as (
            select * from {{ ref('re_data_base_metrics') }}
        )
        select 
            (select json_agg(row_to_json(z)) from z_score_cte z) as anomalies,
            (select json_agg(row_to_json(b)) from base_metrics_cte b) as metrics,
            {{two_dollar_sign}}{{ dbt_graph }}{{two_dollar_sign}} as graph,
            {{ dbt_utils.current_timestamp_in_utc() }} as generated_at
        
    {% endset %}

    {% set overview_result = run_query(overview_query) %}
    {% do overview_result.to_json('target/re_data_overview.json') %}

    {{ overview_query }}

{% else %}
    {{ re_data.empty_overview_table() }}
{% endif %}