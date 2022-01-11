
with median_value_cte as 
(
select
{{-re_data.clean_impute_values('age', agg_function = 'median' )-}} as age_imputed_median

from {{- ref('customers_to_impute') -}}
)
select

    src.*
    , age_imputed_median
    , {{-re_data.clean_impute_values('age', agg_function = 'avg' )-}} as age_imputed_avg
    , {{-re_data.clean_impute_values('age', agg_function = 'max' )-}} as age_imputed_max
    , {{-re_data.clean_impute_values('age', agg_function = 'min' )-}} as age_imputed_min

from {{- ref('customers_to_impute') -}} src, median_value_cte

