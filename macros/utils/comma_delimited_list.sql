{% macro comma_delimited_list(args) %}
    {%- for arg in args %} 
        {{- arg -}} {{- ", " if not loop.last else "" -}}
    {% endfor %}
{% endmacro %}}