{{
    config(
        materialized='incremental',
        unique_key = 'id'
    )
}}

-- depends_on: {{ ref('re_data_columns') }}
-- depends_on: {{ ref('re_data_tables') }}
-- depends_on: {{ ref('re_data_last_base_metrics') }}
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

    {%- for mtable in tables %}
        {% set table_name = row_value(mtable, 'table_name') %}
        {% set time_filter = row_value(mtable, 'time_filter') %}

        {% set columns_query %}
            select * from {{ ref('re_data_columns') }}
            where table_name = '{{ table_name }}'
        {% endset %}

        {% set columns = run_query(columns_query) %}

        {{ log('processing for table name ' ~ table_name, True)}}

        {% set columns_to_query = [] %}
        {% set size = columns_to_query| length %}

        {% for column in columns %}
            {% do columns_to_query.append(column) %}
            {% set columns_size = columns_to_query| length %}

            {% if columns_size == 12 %}
                {%- set insert_stats_query = get_insert_metrics_query(table_name, time_filter, columns_to_query) -%}

                {% if insert_stats_query %}
                    {% do run_query(insert_stats_query) %}
                {% endif %}
                {% do columns_to_query.clear() %}
            {% endif %}

        {% endfor %}

        {%- set insert_stats_query = get_insert_metrics_query(table_name, time_filter, columns_to_query, table_level=True) -%}
        {% do run_query(insert_stats_query) %}

        {{ log('processing finished for table name ' ~ table_name, True)}}

    {% endfor %}

    with 

    with_time_window as (
        select
            *,
            {{ time_window_start() }} as time_window_start,
            {{ time_window_end() }} as time_window_end
        from {{ ref('re_data_last_base_metrics') }}
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
        cast (
            {{ interval_length_sec('time_window_start', 'time_window_end') }} as {{ integer_type() }}
        ) as interval_length_sec,
        {{- dbt_utils.current_timestamp_in_utc() -}} as computed_on
    from with_time_window
{% endif %}