{{
    config(
        materialized='incremental',
        unique_key = 'id',
        on_schema_change='sync_all_columns',
    )
}}

-- depends_on: {{ ref('re_data_columns') }}
-- depends_on: {{ ref('re_data_last_base_metrics_thread0') }}
-- depends_on: {{ ref('re_data_last_base_metrics_thread1') }}
-- depends_on: {{ ref('re_data_last_base_metrics_thread2') }}
-- depends_on: {{ ref('re_data_last_base_metrics_thread3') }}
-- depends_on: {{ ref('re_data_last_base_metrics_part0') }}
-- depends_on: {{ ref('re_data_last_base_metrics_part1') }}
-- depends_on: {{ ref('re_data_last_base_metrics_part2') }}
-- depends_on: {{ ref('re_data_last_base_metrics_part3') }}
-- depends_on: {{ ref('re_data_run_started_at') }}
-- depends_on: {{ ref('re_data_monitored') }}

with 

with_time_window as (
    {% set parts = ['0','1','2','3'] %}
    {% for part in parts %}
        {% set ref_name = 're_data_last_base_metrics_part' + part %}
        select
            *,
            {{ time_window_start() }} as time_window_start,
            {{ time_window_end() }} as time_window_end
        from {{ ref(ref_name) }}
        {%- if not loop.last %} union all {%- endif %}
    {% endfor %}
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