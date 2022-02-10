{{
    config(
        materialized='incremental',
    )
}}

{{ re_data.empty_test_history() }}