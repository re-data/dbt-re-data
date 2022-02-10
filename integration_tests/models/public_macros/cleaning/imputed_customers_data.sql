
{#postgres only #}
{% if target.type == 'postgres' %} 

with median_value_cte as 

-- separate median calculation for postgres, some details can be found in the fivetran's docs 
--For Postgres, this macro uses the aggregate, as it does not support a percentile window function. Thus, you will need to add a target-dependent group by in the query you are calling this macro in.
-- https://hub.getdbt.com/fivetran/fivetran_utils/0.2.9/
(
select
{{-re_data.clean_impute_values('age', replace_with = 'median', replace_all_values = true )}} as age_precalculated_median
from {{ ref('customers_to_impute') }}
)

{% endif %}
{#postgres only #}
select

    src.*
    , 
    {% if target.type == 'postgres' %} 
        {{-re_data.clean_impute_values('age', replace_with = 'age_precalculated_median ', list_of_values_to_replace = [44,51] )}}
    {% else %}
        {{-re_data.clean_impute_values('age', replace_with = 'median', list_of_values_to_replace = [44,51])}}
    {% endif %}
     as age_imputed_median
    , {{-re_data.clean_impute_values('age', replace_with = 'avg', list_of_values_to_replace = [33,60] )}} as age_imputed_avg
    , {{-re_data.clean_impute_values('age', replace_with = 'max' )}} as age_imputed_max
    , {{-re_data.clean_impute_values('age', replace_with = 'min' )}} as age_imputed_min

from {{ ref('customers_to_impute') }} src 
{% if target.type == 'postgres' %} , median_value_cte {% endif %}

