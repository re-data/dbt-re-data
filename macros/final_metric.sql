{% macro final_metric(metric) %}

select * from {{ ref('re_base_metrics') }} where metric = '{{metric}}'

{% endmacro %}