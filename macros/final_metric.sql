{% macro final_metric(metric) %}
    {{ adapter.dispatch('final_metric')(metric) }}
{% endmacro %}

{% macro default__final_metric(metric) %}

select * from {{ ref('re_base_metrics') }} where metric = '{{metric}}'

{% endmacro %}