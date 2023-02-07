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
    cast ({{dbt.current_timestamp()}} as {{ timestamp_type() }} ) as detected_time
from
    {{ ref('re_data_columns')}} cols, {{ ref('re_data_selected')}} tables
where
    cols.name = tables.name and cols.schema = tables.schema and cols.database = tables.database
)

select
    cast ({{ dbt_utils.generate_surrogate_key([
      'table_name',
      'column_name',
      'detected_time'
    ]) }} as {{ string_type() }} ) as id,
    table_name,
    column_name,
    data_type,
    is_nullable,
    detected_time
from columns