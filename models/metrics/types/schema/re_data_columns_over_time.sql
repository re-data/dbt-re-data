{{
    config(
        materialized='incremental',
        unique_key = 'id',
        on_schema_change='sync_all_columns',
    )
}}


with columns as (

select
    {{ full_table_name('cols.name', 'cols.schema', 'cols.database') }} as table_name,
    cols.column_name,
    cols.data_type,
    cols.is_nullable,
    cast ({{dbt_utils.current_timestamp_in_utc()}} as {{ timestamp_type() }} ) as detected_time
from
    {{ ref('re_data_columns')}} cols, {{ ref('re_data_monitored')}} tables
where
    cols.name = tables.name and cols.schema = tables.schema and cols.database = tables.database
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