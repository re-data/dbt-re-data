
{% macro dict_from_list(el_list) %}

    {% set for_cols_dict = {} %}
    {% for col in el_list %}
        {% do for_cols_dict.update({col: True})%}
    {% endfor %}
    {% do return(for_cols_dict) %}

{% endmacro %}