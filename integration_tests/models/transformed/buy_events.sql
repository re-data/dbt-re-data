{{
    config(
        re_data_monitored=true,
        re_data_time_filter='creation_time',
        re_data_anomaly_detector={'name': 'z_score', 'threshold': 0.5},
        materialized='table',
        tags=['testtag']
    )
}}
select *
from {{ ref('sample_with_anomaly') }}
where event_type = 'buy'