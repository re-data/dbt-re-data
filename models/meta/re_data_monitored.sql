{{
    config(
        materialized='table',
        unique_key = 'table_name'
    )
}}

{% if execute %}
    with code_monitored as (
        {{ get_tables_from_config()}}
    )

    select * from code_monitored where actively_monitored = true
{% endif %}

