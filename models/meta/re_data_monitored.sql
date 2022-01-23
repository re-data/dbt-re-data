{{
    config(
        materialized='table',
        unique_key = 'table_name',
        post_hook="{% if execute %}{{ insert_into_monitored() }}{% endif %}"
    )
}}

{{ empty_code_monitored() }}
 
