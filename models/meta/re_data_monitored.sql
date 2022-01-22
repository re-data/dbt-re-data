{{
    config(
        materialized='table',
        unique_key = 'table_name'
        post_hook="{{ insert_into_monitored() }}"
    )
}}

{{ empty_code_monitored() }}
 
