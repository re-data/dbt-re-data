{% macro clean_capitalize_words(column_name) %}
    initcap( {{column_name}} )
{% endmacro %}