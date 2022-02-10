{{
    config(re_data_monitored=true, re_data_time_filter='creation_time', materialized='table')
}}
select *
from {{ ref('sample_with_anomaly') }}
where event_type = 'buy'