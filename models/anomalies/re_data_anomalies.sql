{{
    config(
        materialized='view'
    )
}}
select
    *,
    {{ re_data.generate_anomaly_message('column_name', 'metric', 'last_value', 'last_avg') }} as message,
    {{ re_data.generate_metric_value_text('metric', 'last_value') }} as last_value_text
from
    {{ ref('re_data_z_score')}}
where
    abs(z_score_value) > {{ var('re_data:alerting_z_score') }}
