{{
    config(
        materialized='incremental',
        on_schema_change='sync_all_columns',
    )
}}

{{ re_data.empty_test_history() }}