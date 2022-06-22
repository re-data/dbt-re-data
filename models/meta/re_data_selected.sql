
select 
    name, schema, database, time_filter, time_filter_data_type, metrics, columns, anomaly_detector, owners
from {{ ref('re_data_monitored')}}
where 
    selected = true
