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
    {% set dbt_graph = tojson(graph) %}
    
    {% set overview_query %}
        with json_metrics
        as ( select {{ (row_to_json(ref('re_data_base_metrics'))) }} as json_row
           from {{ ref('re_data_base_metrics') }} ),
        json_anomalies as ( select {{ (row_to_json(ref('re_data_alerting'))) }} as json_row 
            from {{ ref('re_data_alerting') }} ),
        json_schema_changes as ( select {{ (row_to_json(ref('re_data_schema_changes'))) }} as json_row 
            from {{ ref('re_data_schema_changes') }} )
    
    select 
        (select {{ agg_to_single_aray('json_row') }} from json_anomalies) as anomalies,
        (select {{ agg_to_single_aray('json_row') }} from json_metrics) as metrics,
        (select {{ agg_to_single_aray('json_row') }} from json_schema_changes) as schema_changes,
        {{ quote_text(dbt_graph)}} graph,
        {{ dbt_utils.current_timestamp_in_utc() }} as generated_at

    {% endset %}

    {% set overview_result = run_query(overview_query) %}
    {% do overview_result.to_json('target/re_data_overview.json') %}

    {{ overview_query }}
    
    
{% else %}
    {{ re_data.empty_overview_table() }}
{% endif %}