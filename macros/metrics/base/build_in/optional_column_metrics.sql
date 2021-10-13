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
    {{ re_data_metric_regex_count(context.column_name, context.config.regex) }}
{% endmacro %}

{% macro re_data_metric_match_regex_percent(context) %}
    {{ percentage_formula(re_data_metric_match_regex(context), re_data_metric_row_count()) }}
{% endmacro %}

{% macro re_data_metric_not_match_regex(context) %}
    {{ re_data_metric_row_count() }} - {{ re_data_metric_regex_count(context.column_name, context.config.regex) }}
{% endmacro %}

{% macro re_data_metric_not_match_regex_percent(context) %}
    {{ percentage_formula(re_data_metric_not_match_regex(context), re_data_metric_row_count()) }}
{% endmacro %}

{% macro re_data_metric_distinct_values(context) %}
    {{ distinct_values(context) }}
{% endmacro %}

{% macro distinct_values(context) %}
    {{ adapter.dispatch('distinct_values', 're_data')(context) }}
{% endmacro %}

{% macro default__distinct_values(context) %}
    coalesce(
        count(distinct {{ context.column_name }} )
    , 0)
{% endmacro %}

{% macro postgres__distinct_values(context) %}
    {# /* In postgres, its faster to count distinct values in a column by selecting then counting in separate steps */ #}
    with temp_table as (
            select distinct {{ context.column_name }} from {{ context.table_name }}
            where {{ in_time_window(context.time_filter) }}
        )
    select coalesce(count(*), 0) from temp_table
{% endmacro %}

{% macro re_data_metric_approx_distinct_values(context) %}
    {{ approx_distinct_values(context) }}
{% endmacro %}

{% macro approx_distinct_values(context) %}
    {{ adapter.dispatch('approx_distinct_values', 're_data')(context) }}
{% endmacro %}

{% macro default__approx_distinct_values(context) %}
    {# /* No approximate distinct count in postgres so we default to using a distinct count */ #}
    {{ re_data_metric_distinct_values(context) }}
{% endmacro %}

{% macro redshift__approx_distinct_values(context) %}
    approximate {{ re_data_metric_distinct_values(context.column_name) }}
{% endmacro %}

{% macro bigquery__approx_distinct_values(context) %}
    approx_count_distinct({{ context.column_name }})
{% endmacro %}

{% macro snowflake__approx_distinct_values(context) %}
    approx_count_distinct({{ context.column_name }})
{% endmacro %}

{% macro re_data_metric_duplicate_values(context) %}
        with temp_table as (
            select {{ context.column_name }} from {{ context.table_name }}
            where {{ in_time_window(context.time_filter) }}
            group by {{ context.column_name }}
            having count(1) > 1
        )
        select coalesce(count(*), 0) from temp_table
{% endmacro %}

{% macro re_data_metric_duplicate_rows(context) %}
        with temp_table as (
            select {{ context.column_name }}, count(1) as row_count from {{ context.table_name }}
            where {{ in_time_window(context.time_filter) }}
            group by {{ context.column_name }}
            having count(1) > 1
        )
        select coalesce(sum(row_count), 0) from temp_table
{% endmacro %}

{% macro re_data_metric_unique_rows(context) %}
        with temp_table as (
            select {{ context.column_name }}, count(1) as row_count from {{ context.table_name }}
            where {{ in_time_window(context.time_filter) }}
            group by {{ context.column_name }}
            having count(1) = 1
        )
        select coalesce(sum(row_count), 0) from temp_table
{% endmacro %}