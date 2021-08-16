-- depends_on: {{ ref('re_data_columns') }}
-- depends_on: {{ ref('re_data_last_base_metrics_part0') }}

{{
    config(
        materialized='table',
    )
}}

{{ compute_metrics_for_tables(0, 're_data_last_base_metrics_part0') }}

{{ empty_last_base_metrics() }}