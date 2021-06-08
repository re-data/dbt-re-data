select
    *
from
    {{ ref('re_data_z_score')}}
where
    abs(z_score_value) > {{ var('re_data:alerting_z_score') }}
