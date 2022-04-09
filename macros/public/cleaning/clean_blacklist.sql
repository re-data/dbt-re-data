{% macro generate_blacklist_pattern(chars_to_blacklist) %}
    {% set pattern = [] %}
    {% for char in chars_to_blacklist %}
        {% set expr = '(' + char + ')' %}
        {% do pattern.append(expr) %}
    {% endfor %}

    {{ return(pattern | join('|')) }}
{% endmacro %}

{%- macro clean_blacklist(column_name, chars_to_blacklist, replacement) -%}
    {% set pattern_string = re_data.generate_blacklist_pattern(chars_to_blacklist) %}

    {{ adapter.dispatch('clean_blacklist', 're_data')(column_name, pattern_string, replacement) }}
{%- endmacro -%}

{%- macro default__clean_blacklist(column_name, pattern_string, replacement) -%}
    regexp_replace( {{ column_name }}, '{{ pattern_string }}', '{{ replacement }}')
{%- endmacro -%}

{%- macro postgres__clean_blacklist(column_name, pattern_string, replacement) -%}
    regexp_replace( {{ column_name }}, '{{ pattern_string }}', '{{ replacement }}', 'g')
{%- endmacro -%}

{%- macro redshift__clean_blacklist(column_name, pattern_string, replacement) -%}
    regexp_replace( {{ column_name }}, '{{ pattern_string }}', '{{ replacement }}')
{%- endmacro -%}

{%- macro bigquery__clean_blacklist(column_name, pattern_string, replacement) -%}
    regexp_replace( {{ column_name }}, """{{ pattern_string }}""", '{{ replacement }}')
{%- endmacro -%}