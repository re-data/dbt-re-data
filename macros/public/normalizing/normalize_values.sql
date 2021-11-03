{% macro is_dbt_relation(obj) %}
    {{ return (obj is mapping and obj.get('metadata', {}).get('type', '').endswith('Relation') )}}
{% endmacro %}

{% macro normalize_expression_cte(reference_table) %}
    with target_table as (
        {% if re_data.is_dbt_relation(reference_table) or reference_table is string %}
            select * from {{ reference_table }}
        {% elif reference_table is mapping %}
            {% for key, value in reference_table.items() %}
                select '{{key}}' as source, '{{value}}' as target
                {% if not loop.last %}union all{% endif %}
            {% endfor %}
        {% endif %}
    )
{% endmacro %}

{%- macro normalize_values(source_relation, column_name, reference_table) -%}
    ( 
        {{ re_data.normalize_expression_cte(reference_table) }}
        
        select s.*, 
        case when t.source is null
                then s.{{column_name}}
            else t.target
            end as {{ column_name + '__normalized'}} 
        from {{ source_relation }} s
        left join target_table t 
        on t.source = s.{{column_name}}
    )
{%- endmacro -%}

