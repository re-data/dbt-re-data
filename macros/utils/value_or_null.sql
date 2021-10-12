
{% macro str_or_null(value) %}
    {% if value %} '{{value}}' {% else %} null {% endif %}
{% endmacro %}

{% macro bool_or_null(value) %}
    {% if value is not none %} {{ value }} {% else %} null {% endif %}
{% endmacro %}