{{
    config(
        materialized='incremental'
    )
}}

with new_tables as (
    select
        table_name
    from
        {{ref('re_monitored_columns')}}
    {%- if is_incremental() %}
    where
        table_name not in (
            select distinct table_name from {{this}} 
        )
    {%- endif %}
    group by 
        table_name
),

new_time_columns as (
    select 
        new_tables.table_name,
        columns.time_filter
    from
        new_tables,
        {{ref('re_monitored_columns')}} as columns
    where
        columns.table_name = new_tables.table_name and
        columns.time_filter is not null
),

order_by_rank as (
    select
        table_name,
        time_filter,
        row_number () over (partition by table_name order by time_filter) as rank_num
    from 
        new_time_columns
)

select
    table_name,
    time_filter,
    true as actively_monitored,
    {{dbt_utils.current_timestamp_in_utc()}} as detected_time
from
    order_by_rank
where
    rank_num = 1

