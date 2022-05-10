{{
    config(
        materialized='table',
    )
}}

{{ re_data.empty_last_table_samples() }}