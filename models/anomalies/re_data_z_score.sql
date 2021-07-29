{{
    config(
        materialized='incremental',
        unique_key = 'id'
    )
}}

with z_score_without_id as (

    select
        stats.table_name as table_name,
        stats.column_name as column_name,
        stats.metric as metric,
        stats.interval_length_sec,
        (last_metric.last_value - stats.last_avg) / (stats.last_stddev + 0.0000000001) as z_score_value,
        last_metric.last_value as last_value,
        stats.last_avg as last_avg,
        stats.last_stddev as last_stddev,
        {{ time_window_end() }} as time_window_end,
        {{dbt_utils.current_timestamp_in_utc()}} as computed_on
    from
        {{ ref('re_data_last_stats') }} as stats,
        {{ ref('re_data_last_metrics') }} as last_metric
    where
        stats.table_name = last_metric.table_name and
        stats.column_name = last_metric.column_name and
        stats.metric = last_metric.metric and
        (
            stats.interval_length_sec = last_metric.interval_length_sec or
            (stats.interval_length_sec is null and last_metric.interval_length_sec is null)
        ) and
        last_metric.last_value is not null and
        stats.last_avg is not null and
        stats.last_stddev is not null
    )

select
    {{ dbt_utils.surrogate_key([
      'table_name',
      'column_name',
      'metric',
      'interval_length_sec',
      'time_window_end'
    ]) }} as id,
    table_name,
    column_name,
    metric,
    z_score_value,
    last_value,
    last_avg,
    last_stddev,
    time_window_end,
    interval_length_sec,
    computed_on

from z_score_without_id