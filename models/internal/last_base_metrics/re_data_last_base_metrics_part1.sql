-- depends_on: {{ ref('re_data_columns') }}

{{
    config(
        materialized='table',
    )
}}

{{ empty_last_base_metrics() }}

