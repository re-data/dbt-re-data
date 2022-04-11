{{
    config(
        materialized='incremental',
        unique_key = 'id',
        on_schema_change='sync_all_columns',
    )
}}

-- depends_on: {{ ref('re_data_run_started_at') }}
-- depends_on: {{ ref('re_data_columns_over_time') }}
-- depends_on: {{ ref('re_data_monitored') }}

{% if execute and not re_data.in_compile() %}
    {% set last_data_points %} 
        select
            distinct detected_time
        from {{ ref('re_data_columns_over_time') }}
        order by
        detected_time desc limit 2;
    {% endset %}

    {% set detected_times = run_query(last_data_points) %}

    {% set times_list = detected_times.columns[0].values() %}
    {% set most_recent_time = times_list[0] %}

    {% if times_list | length > 1 %}
        {% set prev_most_recent = times_list[1] %}
    {% else %}
        {% set prev_most_recent = times_list[0] %}
    {% endif %}
{% else %}
    {% set times_list = () %}
{% endif %}

{% if times_list == () %}
    {{
        re_data.empty_table_generic([
            ('id', 'string'),
            ('table_name', 'string'),
            ('operation', 'string'),
            ('column_name', 'string'),
            ('data_type', 'string'),
            ('is_nullable', 'boolean'),
            ('prev_column_name', 'string'),
            ('prev_data_type', 'string'),
            ('prev_is_nullable', 'boolean'),
            ('detected_time', 'timestamp')
            ])
    }}
{% else %}

    with curr_monitored_schema as (
        select * from {{ ref('re_data_columns_over_time')}}
        where detected_time = '{{ most_recent_time }}'
        and table_name in (
            select {{ full_table_name('name', 'schema', 'database') }} from {{ ref('re_data_monitored')}}
        )
    ),


    prev_monitored_schema as (
        select * from {{ ref('re_data_columns_over_time')}}
        where detected_time = '{{ prev_most_recent}}'
        and table_name in (
            select {{ full_table_name('name', 'schema', 'database') }} from {{ ref('re_data_monitored')}}
        )
    ),

    all_changes as (
            (
            select
                curr.table_name as table_name,
                'type_change' as operation,
                curr.column_name as column_name,
                curr.data_type as data_type,
                curr.is_nullable as is_nullable,

                prev.column_name as prev_column_name,
                prev.data_type as prev_data_type,
                prev.is_nullable as prev_is_nullable
            
            from curr_monitored_schema curr inner join prev_monitored_schema prev on (curr.table_name = prev.table_name and curr.column_name = prev.column_name)
            where
                curr.data_type != prev.data_type or 
                curr.is_nullable != prev.is_nullable
            )

        union all

        (

            select
                curr.table_name as table_name,
                'column_added' as operation,
                curr.column_name as column_name,
                curr.data_type as data_type,
                curr.is_nullable as is_nullable,

                null as prev_column_name,
                null as prev_data_type,
                null as prev_is_nullable
            
            from curr_monitored_schema curr left join prev_monitored_schema prev on (curr.table_name = prev.table_name and curr.column_name = prev.column_name)
            where prev.table_name is null and prev.column_name is null
            {# note: when a column is added, make sure we only detect for models that were previously monitored,
            this avoids a situation where a newly monitored model has all its columns detected with 'column_added' operation#}
            and curr.table_name in (
                select table_name from prev_monitored_schema
            )
        
        )

        union all

        (

            select
                prev.table_name as table_name,
                'column_removed' as operation,
                null as column_name,
                null as data_type,
                null as is_nullable,

                prev.column_name as prev_column_name,
                prev.data_type as prev_data_type,
                prev.is_nullable as prev_is_nullable
            
            from prev_monitored_schema prev left join curr_monitored_schema curr on (curr.table_name = prev.table_name and curr.column_name = prev.column_name)
            where curr.table_name is null and curr.column_name is null

        )
    ),

    all_with_time as (
        select
            all_changes.table_name,
            all_changes.operation,
            all_changes.column_name,
            all_changes.data_type,
            all_changes.is_nullable,
            all_changes.prev_column_name,
            all_changes.prev_data_type,
            all_changes.prev_is_nullable,
            {{dbt_utils.current_timestamp_in_utc()}} as detected_time
        from all_changes
    )

    select 
        {{ dbt_utils.surrogate_key([
        'table_name',
        'column_name',
        'detected_time'
        ]) }} as id,
        table_name,
        operation,
        column_name,
        data_type,
        is_nullable,
        prev_column_name,
        prev_data_type,
        prev_is_nullable,
        detected_time
    from all_with_time
    
{% endif %}