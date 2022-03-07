{% macro split_and_return_nth_value(column_name, delimiter, ordinal) -%}
    {{ adapter.dispatch('split_and_return_nth_value', 're_data')(column_name, delimiter, ordinal) }}
{%- endmacro %}

{% macro default__split_and_return_nth_value(column_name, delimiter, ordinal) -%}
    split_part({{ re_data.clean_blacklist(column_name, ['"', '`'], '') }}, '{{ delimiter }}', {{ ordinal }})
{%- endmacro %}