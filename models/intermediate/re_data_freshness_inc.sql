{{
    config(
        materialized='incremental',
        unique_key = 'id'
    )
}}

-- depends_on: {{ ref('re_data_columns') }}
-- depends_on: {{ ref('re_data_tables') }}
{% set tables = run_query(get_tables()) %}

{# /* in compile context we don't have access to tables */ #}
{% if execute %}
    {% set table_values = tables.rows.values() %}
{% else %}
    {% set table_values = () %}
{% endif %}

{% if table_values == () %}
    {{ dummy_empty_fressness_table() }}
{% else %}

    with without_forced_types as (
        {%- for mtable in tables %}
            {% set table_name = row_value(mtable, 'table_name') %}
            {% set time_filter = row_value(mtable, 'time_filter') %}
            select
                cast ('{{table_name}}' as {{ string_type() }} ) as table_name,
                cast ('' as {{ string_type() }}) as column_name,
                cast ( 'freshness' as {{ string_type() }}) as metric,
                {{freshness_expression(time_filter)}} as value,
                {{ time_window_end() }} as time_window_end,
                {{- dbt_utils.current_timestamp_in_utc() -}} as computed_on
            from
                {{table_name}}
            where 
                {{before_time_window_end(time_filter)}}

        {%- if not loop.last -%} union all {%- endif %}
        {%- endfor %}
    )

    select
        {{ dbt_utils.surrogate_key([
            'table_name',
            'column_name',
            'metric',
            'time_window_end'
        ]) }} as id,
        cast (table_name as {{ string_type() }} ) as table_name,
        cast (column_name as {{ string_type() }} ) as column_name,
        cast (metric as {{ string_type() }} ) as metric,
        cast (value as {{ numeric_type() }} ) as value,
        cast (time_window_end as {{ timestamp_type() }} ) as time_window_end,
        cast (null as {{ integer_type() }} ) as interval_length_sec,
        cast (computed_on as {{ timestamp_type() }} ) as computed_on
    from without_forced_types

{% endif %}