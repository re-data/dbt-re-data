
select
    {{ clean_table_name('table_name') }} as table_name,
    {{ clean_column_name('column_name') }} as column_name,
    metric,
    anomaly_detector,
    interval_length_sec

from {{ ref('re_data_anomalies') }}