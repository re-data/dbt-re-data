{{
    config(
        materialized='incremental',
        unique_key = 'id'
    )
}}


with columns as (

select
    cols.table_name,
    cols.column_name,
    cols.data_type,
    cols.is_nullable,
    cast ({{dbt_utils.current_timestamp_in_utc()}} as {{ timestamp_type() }} ) as detected_time
from
    {{ ref('re_data_columns')}} cols, {{ ref('re_data_tables')}} tables
where
    cols.table_name = tables.table_name and
    tables.actively_monitored = true
)

select
    {{ dbt_utils.surrogate_key([
      'table_name',
      'column_name',
      'detected_time'
    ]) }} as id,
    table_name,
    column_name,
    data_type,
    is_nullable,
    detected_time
from columns