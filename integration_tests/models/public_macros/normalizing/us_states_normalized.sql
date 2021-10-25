with us_states_normalization_sql as (
    select * from {{ ref('us_states_normalization') }}
)

{% set us_states_mapping = {'N.Y.': 'New York'} %}

select * from {{ re_data.normalize_values(ref('abbreviated_us_states'), 'state', 'us_states_normalization_sql') }} s
{# select * from {{ re_data.normalize_values(ref('abbreviated_us_states'), 'state', ref('us_states_normalization')) }} s #}
{# select * from {{ re_data.normalize_values(ref('abbreviated_us_states'), 'state', us_states_mapping) }} s #}