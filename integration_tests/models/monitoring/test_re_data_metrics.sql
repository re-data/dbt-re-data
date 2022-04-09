
select
    {{ clean_table_name('table_name') }} as table_name,
    {{ clean_column_name('column_name') }} as column_name,
    metric,
    time_window_start,
    time_window_end,
    {{ to_big_integer('value') }},
    interval_length_sec

from {{ ref('re_data_metrics') }}