
{% macro re_data_metric_max(context) %}
    {% set column_name = context.get('column_name') %}
    max({{column_name}})
{% endmacro %}

{% macro re_data_metric_min(context) %}
    {% set column_name = context.get('column_name') %}
    min({{column_name}})
{% endmacro %}

{% macro re_data_metric_avg(context) %}
    {% set column_name = context.get('column_name') %}
    avg(cast ({{column_name}} as {{ numeric_type() }}))
{% endmacro %}

{% macro re_data_metric_stddev(context) %}
    {% set column_name = context.get('column_name') %}
    stddev(cast ( {{column_name}} as {{ numeric_type() }}))
{% endmacro %}

{% macro re_data_metric_variance(context) %}
    {% set column_name = context.get('column_name') %}
    variance(cast ( {{column_name}} as {{ numeric_type() }}))
{% endmacro %}

{% macro re_data_metric_max_length(context) %}
    {% set column_name = context.get('column_name') %}
    max(length({{column_name}}))
{% endmacro %}

{% macro re_data_metric_min_length(context) %}
    {% set column_name = context.get('column_name') %}
    min(length({{column_name}}))
{% endmacro %}

{% macro re_data_metric_avg_length(context) %}
    {% set column_name = context.get('column_name') %}
    avg(cast (length( {{column_name}} ) as {{ numeric_type() }}))
{% endmacro %}

{% macro re_data_metric_nulls_count(context) %}
    {% set column_name = context.get('column_name') %}
    coalesce(
        sum(
            case when {{column_name}} is null
                then 1
            else 0
            end
        ), 0
    )
{% endmacro %}

{% macro re_data_metric_missing_count(context) %}
    {% set column_name = context.get('column_name') %}
    coalesce(
        sum(
            case 
            when {{column_name}} is null
                then 1
            when {{column_name}} = ''
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

{% macro re_data_metric_regex_count(column_name, pattern) %}
    coalesce(
        sum(
            case when {{ regex_match_expression(column_name, pattern) }}
                then 1
            else 0
            end
        ), 0
    )
{% endmacro %}

{% macro re_data_metric_match_regex(context) %}
    {% set column_name = context.get('column_name') %}
    {% set pattern = context.get('config', {}).get('regex') %}
    {{ re_data_metric_regex_count(column_name, pattern) }}
{% endmacro %}

{% macro re_data_metric_match_regex_percent(context) %}
    {{ percentage_formula(re_data_metric_match_regex(context), re_data_metric_row_count()) }}
{% endmacro %}

{% macro re_data_metric_not_match_regex(context) %}
    {% set column_name = context.get('column_name') %}
    {% set pattern = context.get('config', {}).get('regex') %}
    {{ re_data_metric_regex_count(column_name, pattern) }}
{% endmacro %}

{% macro re_data_metric_not_match_regex_percent(context) %}
    {{ percentage_formula(re_data_metric_not_match_regex(context), re_data_metric_row_count()) }}
{% endmacro %}

