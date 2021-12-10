{{
    config(
        materialized='table',
        unique_key = 'generated_at'
    )
}}

-- depends_on: {{ ref('re_data_alerting') }}
-- depends_on: {{ ref('re_data_base_metrics') }}
-- depends_on: {{ ref('re_data_schema_changes') }}
-- depends_on: {{ ref('re_data_columns') }}

{% if execute %}
    {% set dbt_graph = tojson(graph) %}
    
        with json_metrics
        as ( select {{ (row_to_json(ref('re_data_base_metrics'))) }} as json_row
           from {{ ref('re_data_base_metrics') }} ),
        json_anomalies as ( select {{ (row_to_json(ref('re_data_alerting'))) }} as json_row 
            from {{ ref('re_data_alerting') }} ),
        casted_schem_changes as (
            select id, table_name, operation, column_name, data_type, {{ bool_to_string('is_nullable') }}, prev_column_name, prev_data_type, {{ bool_to_string('prev_is_nullable') }}, detected_time
            from {{ ref('re_data_schema_changes') }}
        ),
        json_schema_changes as ( select {{ (row_to_json(ref('re_data_schema_changes'))) }} as json_row 
            from casted_schem_changes),
        casted_columns as (
            select id, table_name, column_name, data_type, {{ bool_to_string('is_nullable') }}, {{ bool_to_string('is_datetime') }}, time_filter from {{ ref('re_data_columns') }} 
        ),
        json_schema as ( select {{ row_to_json(ref('re_data_columns')) }} as json_row 
            from casted_columns
        )

        select 'metric', json_row from json_metrics union all
        select 'anomaly', json_row from json_anomalies union all
        select 'schema_change', json_row from json_schema_changes union all
        select 'schema', json_row from json_schema
        select 'graph', dbt_graph
    
{% else %}
    {{ re_data.empty_overview_table() }}
{% endif %}