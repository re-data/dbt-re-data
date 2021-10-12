
{% macro re_data_metric_max(context) %}
    max({{context.column_name}})
{% endmacro %}

{% macro re_data_metric_min(context) %}
    min({{context.column_name}})
{% endmacro %}

{% macro re_data_metric_avg(context) %}
    avg(cast ({{context.column_name}} as {{ numeric_type() }}))
{% endmacro %}

{% macro re_data_metric_stddev(context) %}
    stddev(cast ( {{context.column_name}} as {{ numeric_type() }}))
{% endmacro %}

{% macro re_data_metric_variance(context) %}
    variance(cast ( {{context.column_name}} as {{ numeric_type() }}))
{% endmacro %}

{% macro re_data_metric_max_length(context) %}
    max(length({{context.column_name}}))
{% endmacro %}

{% macro re_data_metric_min_length(context) %}
    min(length({{context.column_name}}))
{% endmacro %}

{% macro re_data_metric_avg_length(context) %}
    avg(cast (length( {{context.column_name}} ) as {{ numeric_type() }}))
{% endmacro %}

{% macro re_data_metric_nulls_count(context) %}
    coalesce(
        sum(
            case when {{context.column_name}} is null
                then 1
            else 0
            end
        ), 0
    )
{% endmacro %}

{% macro re_data_metric_missing_count(context) %}
    coalesce(
        sum(
            case 
            when {{context.column_name}} is null
                then 1
            when {{context.column_name}} = ''
                then 1
            else 0
            end
        ), 0
    )
{% endmacro %}

{% macro re_data_metric_nulls_percent(context) %}
    {{ percentage_formula(re_data_metric_nulls_count(context), re_data_metric_row_count()) }}
{% endmacro %}

{% macro re_data_metric_missing_percent(context) %}
    {{ percentage_formula(re_data_metric_missing_count(context), re_data_metric_row_count()) }}
{% endmacro %}


