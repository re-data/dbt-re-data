{% macro comparison_text(a, b) %}
    case when {{a}} > {{b}} then 'greater than' 
    when {{a}} = {{b}} then 'equal to'
    else 'less than' end
{% endmacro %}