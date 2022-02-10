
{% macro name_in_db(name) %}
    {% set translated = adapter.dispatch('name_in_db', 're_data')(name) %}
    {{ return(translated) }}
    
{% endmacro %}

{% macro default__name_in_db(name) %}
    {{ return(name) }}
{% endmacro %}