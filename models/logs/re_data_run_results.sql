{{
    config(
        materialized='incremental',
    )
}}

{{ re_data.empty_run_results() }}