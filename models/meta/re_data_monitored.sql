{{
    config(
        materialized='table',
        unique_key = 'table_name'
    )
}}
{% if not execute %}
    select * from  {{ ref('re_data_tables') }}

{% else %}

with db_monitored as (
    select
        table_name, time_filter, actively_monitored
    from 
        {{ ref('re_data_tables') }}
)
, code_monitored as (
    {{ get_tables_from_config()}}
),
all_status as (
select 
    dm.table_name as table_name,
    case 
        when cm.table_name is not null then cm.time_filter
             else dm.time_filter
    end as time_filter,
    case 
        when cm.actively_monitored is not null
                then cm.actively_monitored
             else dm.actively_monitored
    end as actively_monitored,
    case
        when cm.metrics is not null
            then cm.metrics
        else '{}'
    end as metrics,
    case 
    when cm.columns is not null
            then cm.columns
        else '[]'
    end as columns
from
    db_monitored dm left join code_monitored cm
    on dm.table_name = cm.table_name
)

select *
from all_status where
    actively_monitored = true

{% endif %}

