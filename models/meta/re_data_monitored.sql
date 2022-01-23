{{
    config(
        materialized='table',
        unique_key = 'table_name',
        post_hook="{% if execute %}{{ pub_insert_into_re_data_monitored() }}{% endif %}"
    )
}}

{{ empty_code_monitored() }}
 
