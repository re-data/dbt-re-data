{# https://github.com/re-data/re-data/issues/148 #}


{# DAMA: Corrective actions - Automated corrections
references for further improvements:
Statistics
https://www.researchgate.net/publication/236635604_Data_Quality_Improvement_by_Imputation_of_Missing_Values
AI Data Repair in H2O
https://www.coursera.org/learn/machine-learning-h2o/lecture/tAtLC/data-repair-2
#}

{% macro clean_impute_values(column_name, agg_function = 'avg') %}

{% if agg_function == 'median' %}
	{{ fivetran_utils.percentile( percentile_field = column_name,  percent='0.5') }}
{% else %}
	{{agg_function}} ({{column_name}}) over ()
{% endif %}


{% endmacro %}