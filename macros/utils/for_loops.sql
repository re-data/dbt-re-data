
{% macro print_list(l) %}
    {% for el in l %}{{el}}{% if not loop.last %},{% endif %}{% endfor %}
{% endmacro %}