{%- macro metric_base_expression(column_name, func) %}

    {%- if func == 'max' %}
        max({{column_name}})
    {% endif %}

    {%- if func == 'min' %}
        min({{column_name}})
    {% endif %}

    {%- if func == 'avg' %}
        avg(cast ({{column_name}} as {{ numeric_type() }}))
    {% endif %}

    {%- if func == 'stddev' %}
        stddev(cast ( {{column_name}} as {{ numeric_type() }}))
    {% endif %}

    {%- if func == 'variance' %}
        variance(cast ( {{column_name}} as {{ numeric_type() }}))
    {% endif %}

    {%- if func == 'max_length' %}
        max(length({{column_name}}))
    {% endif %}

    {%- if func == 'min_length' %}
        min(length({{column_name}}))
    {% endif %}

    {%- if func == 'avg_length' %}
        avg(cast (length( {{column_name}} ) as {{ numeric_type() }}))
    {% endif %}

    {%- if func == 'count_nulls' %}
        coalesce(
            sum(
                case when {{column_name}} is null
                    then 1
                else 0
                end
            ), 0
        )
    {% endif %}

    {%- if func == 'count_missing' %}
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
    {% endif %}

{% endmacro %}