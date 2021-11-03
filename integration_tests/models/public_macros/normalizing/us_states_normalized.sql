with us_states_normalization_cte as (
    select source, target from {{ ref('us_states_normalization') }}
)

{% set us_states_mapping = {'Ala.': 'Alabama', 'Alaska': 'Alaska', 'Ariz.': 'Arizona', 'Ark.': 'Arkansas', 'Calif.': 'California', 'Colo.': 'Colorado', 'Conn.': 'Connecticut',
    'Del.': 'Delaware', 'D.C.': 'District of Columbia', 'Fla.': 'Florida', 'Ga.': 'Georgia', 'Hawaii': 'Hawaii', 'Idaho': 'Idaho', 'Ill.': 'Illinois', 'Ind.': 'Indiana',
    'Iowa': 'Iowa', 'Kans.': 'Kansas', 'Ky.': 'Kentucky', 'La.': 'Louisiana', 'Maine': 'Maine', 'Md.': 'Maryland', 'Mass.': 'Massachusetts', 'Mich.': 'Michigan',
    'Minn.': 'Minnesota', 'Miss.': 'Mississippi', 'Mo.': 'Missouri', 'Mont.': 'Montana', 'Nebr.': 'Nebraska', 'Nev.': 'Nevada', 'N.H.': 'New Hampshire', 'N.J.': 'New Jersey',
    'N.M.': 'New Mexico', 'N.Y.': 'New York', 'N.C.': 'North Carolina', 'N.D.': 'North Dakota', 'Ohio': 'Ohio', 'Okla.': 'Oklahoma', 'Ore.': 'Oregon', 'Pa.': 'Pennsylvania',
    'R.I.': 'Rhode Island', 'S.C.': 'South Carolina', 'S.D.': 'South Dakota', 'Tenn.': 'Tennessee', 'Tex.': 'Texas', 'Utah': 'Utah', 'Vt.': 'Vermont', 'Va.': 'Virginia',
    'Wash.': 'Washington', 'W.Va.': 'West Virginia', 'Wis.': 'Wisconsin', 'Wyo.': 'Wyoming'}
%}


{# 
    We have three ways of passing the source used for normalization
        1. passing a dbt model using ref('') which is of type Relation.
        2. passing a common table expression that contains the source mapping
            Note: model or cte must include "source" and "target" column names used for normalization in 1. & 2. repectively
        3. passing a dictionary of values that map from source -> target ie {[source]: [target]}
 #}

select distinct * from (
    select state, code, state__normalized from {{ re_data.normalize_values(ref('abbreviated_us_states'), 'state', ref('us_states_normalization')) }} s
    union all
    select state, code, state__normalized from {{ re_data.normalize_values(ref('abbreviated_us_states'), 'state', 'us_states_normalization_cte') }} s
    union all
    select state, code, state__normalized from {{ re_data.normalize_values(ref('abbreviated_us_states'), 'state', us_states_mapping) }} s
) as normalized