{% macro final_metric(metric) %}
    {{ adapter.dispatch('final_metric', 're_data')(metric) }}
{% endmacro %}

{% macro default__final_metric(metric) %}

select * from {{ ref('re_data_base_metrics') }} where metric = '{{metric}}'

{% endmacro %}