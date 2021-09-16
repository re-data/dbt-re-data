select *
from {{ ref('sample_with_anomaly') }}
where event_type = 'buy'