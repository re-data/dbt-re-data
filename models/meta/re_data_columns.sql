{{
    config(
        materialized='table'
    )
}}

{% set schemas = var('re_data:schemas') %}
with columns_froms_select as (
    {% for for_schema in schemas %}
        {{ get_monitored_columns(for_schema) }}
    {%- if not loop.last %} union all {%- endif %}
    {% endfor %}
)

select
    cast (table_name as {{ string_type() }} ) as table_name,
    cast (column_name as {{ string_type() }} ) as column_name,
    cast (data_type as {{ string_type() }} ) as data_type,
    cast (case is_nullable when 'YES' then 1 else 0 end as {{ boolean_type() }} ) as is_nullable,
    cast (is_datetime as {{ boolean_type() }} ) as is_datetime,
    cast (time_filter as {{ string_type() }} ) as time_filter
from columns_froms_select