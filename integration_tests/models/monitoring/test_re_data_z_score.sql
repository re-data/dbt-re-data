
{% set values_compare = [
    'z_score_value',
    'modified_z_score_value', 
    'last_value',
    'last_avg',
    'last_stddev',
    'last_median',
    'last_iqr',
    'last_median_absolute_deviation',
    'last_mean_absolute_deviation',
] %}

select
    {{ clean_table_name('table_name') }} as table_name,
    {{ clean_column_name('column_name') }} as column_name,
    metric,
    time_window_end,
    {% for col in values_compare %}{{ to_big_integer(col) }},{% endfor %}
    interval_length_sec

from {{ ref('re_data_z_score') }}