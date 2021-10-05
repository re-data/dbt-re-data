
{% macro re_data_metric_max(column_name) %}
    max({{column_name}})
{% endmacro %}

{% macro re_data_metric_min(column_name) %}
    min({{column_name}})
{% endmacro %}

{% macro re_data_metric_avg(column_name) %}
    avg(cast ({{column_name}} as {{ numeric_type() }}))
{% endmacro %}

{% macro re_data_metric_stddev(column_name) %}
    stddev(cast ( {{column_name}} as {{ numeric_type() }}))
{% endmacro %}

{% macro re_data_metric_variance(column_name) %}
    variance(cast ( {{column_name}} as {{ numeric_type() }}))
{% endmacro %}

{% macro re_data_metric_max_length(column_name) %}
    max(length({{column_name}}))
{% endmacro %}

{% macro re_data_metric_min_length(column_name) %}
    min(length({{column_name}}))
{% endmacro %}

{% macro re_data_metric_avg_length(column_name) %}
    avg(cast (length( {{column_name}} ) as {{ numeric_type() }}))
{% endmacro %}

{% macro re_data_metric_nulls_count(column_name) %}
    coalesce(
        sum(
            case when {{column_name}} is null
                then 1
            else 0
            end
        ), 0
    )
{% endmacro %}

{% macro re_data_metric_missing_count(column_name) %}
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

{% macro re_data_metric_nulls_percent(column_name) %}
    {{ percentage_formula(re_data_metric_nulls_count(column_name), re_data_metric_row_count()) }}
{% endmacro %}

{% macro re_data_metric_missing_percent(column_name) %}
    {{ percentage_formula(re_data_metric_missing_count(column_name), re_data_metric_row_count()) }}
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

{% macro re_data_metric_match_regex(column_name, config) %}
    {% set pattern = config.get('regex') %}
    {{ re_data_metric_regex_count(column_name, pattern) }}
{% endmacro %}

{% macro re_data_metric_match_regex_percent(column_name, config) %}
    {{ percentage_formula(re_data_metric_match_regex(column_name, config), re_data_metric_row_count()) }}
{% endmacro %}

{% macro re_data_metric_not_match_regex(column_name, config) %}
    {% set pattern = config.get('regex') %}
    {{ re_data_metric_regex_count(column_name, pattern) }}
{% endmacro %}

{% macro re_data_metric_not_match_regex_percent(column_name, config) %}
    {{ percentage_formula(re_data_metric_not_match_regex(column_name, config), re_data_metric_row_count()) }}
{% endmacro %}

{% macro re_data_metric_distinct_values(column_name) %}
    count(distinct {{ column_name }} )
{% endmacro %}

{% macro re_data_metric_approx_distinct_values(column_name) %}
    {{ approx_distinct_values(column_name) }}
{% endmacro %}

{% macro approx_distinct_values(column_name) %}
    {{ adapter.dispatch('approx_distinct_values', 're_data')(column_name) }}
{% endmacro %}

{% macro default__approx_distinct_values(column_name) %}
   {{ re_data_metric_distinct_values(column_name) }}
{% endmacro %}

{% macro redshift__approx_distinct_values(column_name) %}
    approximate {{ re_data_metric_distinct_values(column_name) }}
{% endmacro %}

{% macro bigquery__approx_distinct_values(column_name) %}
    approx_count_distinct({{ column_name }})
{% endmacro %}

{% macro snowflake__approx_distinct_values(column_name) %}
   approx_count_distinct({{ column_name }})
{% endmacro %}
