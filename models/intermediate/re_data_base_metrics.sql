{{
    config(
        materialized='incremental',
        unique_key = 'id'
    )
}}

-- depends_on: {{ ref('re_data_columns') }}
-- depends_on: {{ ref('re_data_tables') }}
{%- set tables =  run_query(get_tables()) %}

{# /* in comple context we don't have access to tables */ #}
{% if execute %}
    {% set table_values = tables.rows.values() %}
{% else %}
    {% set table_values = () %}
{% endif %}

{% if table_values == () %}
    {{ dummy_empty_base_metrics_table() }}
{% else %}

    with without_forced_types as (
    {%- set table_results = [] %}

    {%- for mtable in tables %}
        {% set table_name = row_value(mtable, 'table_name') %}
        {% set time_filter = row_value(mtable, 'time_filter') %}

        {%- call statement('metrics', fetch_result=True) -%}
        select
            {{- base_metrics_query(mtable) -}}
        from
            {{table_name}}
        where
            {{ in_time_window(time_filter) }}
        {%- endcall -%}

        {%- set result = load_result('metrics')['table'] -%}
        {%- do table_results.append({'table': table_name, 'result': result}) %}
    {% endfor %}

    {%- for result in table_results %}
        {%- set table_name = result.table %}
        {%- set m_for_table = result.result %}
        {%- for column in m_for_table.columns %}
            {%- set column_value = column.values()[0] %}
            {%- set column_name = column.name %}
            {%- set table_column_name, fun = column_name.split('___') %}
            select 
                '{{table_name}}' as table_name,
                '{{table_column_name}}' as column_name,
                '{{fun}}' as metric,
                cast({{column_value | replace(None, 'NULL')}} as {{ numeric_type() }})  as value,
                {{- time_window_start() -}} as time_window_start,
                {{- time_window_end() -}} as time_window_end,
                {{- dbt_utils.current_timestamp_in_utc() -}} as computed_on
            {%- if not loop.last %} union all {%- endif %}
            {% endfor %}

        {%- if not loop.last %} union all {%- endif %}
    {%- endfor %}

    )

    select
        {{ dbt_utils.surrogate_key([
            'table_name',
            'column_name',
            'metric',
            'time_window_start',
            'time_window_end'
        ]) }} as id,
        cast (table_name as {{ string_type() }} ) as table_name,
        cast (column_name as {{ string_type() }} ) as column_name,
        cast (metric as {{ string_type() }} ) as metric,
        cast (value as {{ numeric_type() }} ) as value,
        cast (time_window_start as {{ timestamp_type() }} ) as time_window_start,
        cast (time_window_end as {{ timestamp_type() }} ) as time_window_end,
        cast (computed_on as {{ timestamp_type() }} ) as computed_on
    from without_forced_types
{% endif %}