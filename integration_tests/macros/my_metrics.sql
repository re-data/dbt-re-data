{% macro re_data_metric_diff(column_name) %}
    max({{column_name}}) - min({{column_name}})
{% endmacro %}


{% macro re_data_metric_buy_count(time_column) %}
    coalesce(
        sum(
            case when event_type = 'buy'
                then 1
            else 0
            end
        ), 0
    )
{% endmacro %}

{% macro re_data_metric_my_custom_table_metric(time_column) %}
    1000
{% endmacro %}


{% macro re_data_metric_distinct_count(column_name) %}
    count(distinct( {{column_name}} ))

{% endmacro %}

{% macro re_data_metric_regexp_test(column_name, config) %}
    {{ regexp_test(column_name, config) }}
{% endmacro %}

{% macro regexp_test(column_name, config) %}
    {{ adapter.dispatch('regexp_test', project_name)(column_name, config) }}
{% endmacro %}

{% macro default__regexp_test(column_name, config) %}
    {% set pattern = config.get('regexp') %}
    coalesce(
        sum(
            case when {{column_name}} ~ '{{pattern}}'
                then 1
            else 0
            end
        ), 0
    )
{% endmacro %}

{% macro bigquery__regexp_test(column_name, config) %}
    {% set pattern = config.get('regexp') %}
    coalesce(
        sum(
            case when regexp_contains({{column_name}}, '{{pattern}}')
                then 1
            else 0
            end
        ), 0
    )
{% endmacro %}

{% macro snowflake__regexp_test(column_name, config) %}
    {% set pattern = config.get('regexp') %}
    coalesce(
        sum(
            case when regexp_like({{column_name | upper}}, '{{pattern}}')
                then 1
            else 0
            end
        ), 0
    )
{% endmacro %}
