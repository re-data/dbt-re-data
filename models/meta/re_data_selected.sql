
select 
    name, schema, database, time_filter, metrics, columns, anomaly_detector, owners
from {{ ref('re_data_monitored')}}
where 
    selected = true