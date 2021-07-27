
{% set last_data_points %} 
select
    distinct detected_time
from {{ ref('re_data_columns_over_time') }}
order by
    detected_time desc limit 2;
{% endset %}

{% set detected_times = run_query(last_data_points) %}

{% if execute %}
    {% set times_list = detected_times.columns[0].values() %}
    {% set most_recent_time = times_list[0] %}

    {% if times_list | length > 1 %}
        {% set prev_most_recent = times_list[1] %}
    {% else %}
        {% set prev_most_recent = times_list[0] %}
    {% endif %}
{% else %}
    {% set most_recent_time = '' %}
    {% set prev_most_recent = '' %}
{% endif %}

with curr_schema as (
    select * from {{ ref('re_data_columns_over_time')}}
    where detected_time = '{{ most_recent_time }}'
),


prev_schema as (
    select * from {{ ref('re_data_columns_over_time')}}
    where detected_time = '{{ prev_most_recent}}'
),

all_changes as (
        (
        select
            curr.table_name as table_name,
            curr.column_name as column_name,
            curr.data_type as data_type,
            curr.is_nullable as is_nullable,

            prev.column_name as prev_column_name,
            prev.data_type as prev_data_type,
            prev.is_nullable as prev_is_nullable
        
        from curr_schema curr inner join prev_schema prev on (curr.table_name = prev.table_name and curr.column_name = prev.column_name)
        where
            curr.data_type != prev.data_type or 
            curr.is_nullable != prev.is_nullable
        )

    union

    (

        select
            curr.table_name as table_name,
            curr.column_name as column_name,
            curr.data_type as data_type,
            curr.is_nullable as is_nullable,

            null as prev_column_name,
            null as prev_data_type,
            null as prev_is_nullable
        
        from curr_schema curr left join prev_schema prev on (curr.table_name = prev.table_name and curr.column_name = prev.column_name)
        where prev.table_name is null and prev.column_name is null
    
    )

    union

    (

        select
            prev.table_name as table_name,
            null as column_name,
            null as data_type,
            null as is_nullable,

            prev.column_name as prev_column_name,
            prev.data_type as prev_data_type,
            prev.is_nullable as prev_is_nullable
        
        from prev_schema prev left join curr_schema curr on (curr.table_name = prev.table_name and curr.column_name = prev.column_name)
        where curr.table_name is null and curr.column_name is null

    )
),

all_with_time (
    select
        *,
        cast (detected_time as {{ timestamp_type() }} ) as detected_time
    from all_changes
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




