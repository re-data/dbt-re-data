{{
    config(
        materialized='table',
        unique_key = 'id'
    )
}}

{% set schemas = var('re_data:schemas') %}
with columns_froms_select as (
    {% for for_schema in schemas %}
        {% set schema_name = re_data.schema_name(for_schema) %}
        {{ get_monitored_columns(schema_name) }}
    {%- if not loop.last %} union all {%- endif %}
    {% endfor %}
)

select
    {{ dbt_utils.surrogate_key([
      'table_name',
      'column_name'
    ]) }} as id,
    cast (table_name as {{ string_type() }} ) as table_name,
    cast (column_name as {{ string_type() }} ) as column_name,
    cast (data_type as {{ string_type() }} ) as data_type,
    cast (case is_nullable when 'YES' then 1 else 0 end as {{ boolean_type() }} ) as is_nullable,
    cast (is_datetime as {{ boolean_type() }} ) as is_datetime,
    cast (time_filter as {{ string_type() }} ) as time_filter
from columns_froms_select