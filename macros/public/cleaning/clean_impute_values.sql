{# https://github.com/re-data/re-data/issues/148 #}



{# 
DAMA: Corrective actions - Automated corrections

possible improvement: partitions

references for further improvements:
Statistics
https://www.researchgate.net/publication/236635604_Data_Quality_Improvement_by_Imputation_of_Missing_Values
AI Data Repair in H2O
https://www.coursera.org/learn/machine-learning-h2o/lecture/tAtLC/data-repair-2
#}


{% macro clean_impute_values(column_name, replace_with = 'avg', list_of_values_to_replace = [], replace_all_values = false) %}
{#
-- parameters:
--  replace_with:
--  in case of 'min', 'max', 'avg' and 'median' the corresponding aggregate replaces the value
--  in other cases, the values are being treated as a general SQL expression

-- for postgres, the median should be precalculated via intermediate cte
#}



{%- if not replace_all_values %}
case
	when 
	not 
	(
		{{column_name}} is null
		{% for val in list_of_values_to_replace -%}
		or {{column_name}} = '{{val}}'
		{% endfor %}
	)
	then {{column_name}}
	else
{%- endif -%}
	{% if replace_with == 'median' %}
		-- for postgres the median should be precalculated via intermediate cte
		-- https://hub.getdbt.com/fivetran/fivetran_utils/0.2.9/
		-- partition_field = 1 median across whole table 
		{{ fivetran_utils.percentile( percentile_field = column_name,  percent='0.5', partition_field = 1) }}
	{% elif replace_with in ['min', 'max' , 'avg'] %}
		{{replace_with}} ({{column_name}}) over ()
	{% else %}
		{{replace_with}}
	{% endif %}
{%- if not replace_all_values -%} end {%- endif %}

{% endmacro %}