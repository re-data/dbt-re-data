{{
    config(
        materialized='table',
    )
}}

select {{ run_started_at.timestamp() * 1000000 }} as run_started_at