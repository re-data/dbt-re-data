{{
    config(
        materialized='incremental',
        unique_key = 'id'
    )
}}


with columns as (

select
    table_name,
    column_name,
    data_type,
    is_nullable,
    cast ({{dbt_utils.current_timestamp_in_utc()}} as {{ timestamp_type() }} ) as detected_time
from
    {{ ref('re_data_columns')}}

)

select
    {{ dbt_utils.surrogate_key([
      'table_name',
      'column_name',
      'dte'
    ]) }} as id,

