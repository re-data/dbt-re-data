
{% set tables =  run_query(get_tables()) %}

{%- for mtable in tables %}
    {% set table_name = row_value(mtable, 'table_name') %}
    {% set time_filter = row_value(mtable, 'time_filter') %}
    select
        '{{table_name}}' as table_name,
        '' as column_name,
        'freshness' as metric,
        {{freshness_expression(time_filter)}} as value,
        {{ time_window_end() }} as time_window_end,
        {{- dbt_utils.current_timestamp_in_utc() -}} as computed_on
    from
        {{table_name}}
    where 
        {{before_time_window_end(time_filter)}}

{%- if not loop.last -%} union all {%- endif %}
{%- endfor %}