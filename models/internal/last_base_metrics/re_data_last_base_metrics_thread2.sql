-- depends_on: {{ ref('re_data_columns') }}
-- depends_on: {{ ref('re_data_last_base_metrics_part2') }}

{{
    config(
        materialized='table',
    )
}}

{{ compute_metrics_for_tables(2, 're_data_last_base_metrics_part2') }}

{{ empty_last_base_metrics() }}