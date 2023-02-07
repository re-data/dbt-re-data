{{
    config(
        materialized='table',
        unique_key = 'table_name',
        on_schema_change='sync_all_columns',
    )
}}

-- depends_on: {{ ref('re_data_last_table_samples') }}
-- depends_on: {{ ref('re_data_last_table_samples_part') }}

select
    table_name,
    sample_data,
    cast ({{- dbt.current_timestamp() -}} as {{ timestamp_type() }}) as sampled_on

from {{ ref('re_data_last_table_samples_part') }}
