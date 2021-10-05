{% macro re_data_metric_distinct_values(context) %}
    {{ distinct_values(context) }}
{% endmacro %}

{% macro distinct_values(context) %}
    {{ adapter.dispatch('distinct_values', 're_data')(context) }}
{% endmacro %}

{% macro default__distinct_values(context) %}
    count(distinct {{ context.column_name }} )
{% endmacro %}

{% macro postgres__distinct_values(context) %}
    {# /* In postgres, its faster to count distinct values in a column by selecting then counting in separate steps */ #}
    (   with temp_table as (
            select distinct {{ context.column_name }} from {{ context.table_name }}
            where {{ in_time_window(context.time_filter) }}
        )
        select count(*) from temp_table
    )
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