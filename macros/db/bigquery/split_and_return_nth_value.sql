{% macro bigquery__split_and_return_nth_value(column_name, delimiter, ordinal) %}
    split({{ re_data.clean_blacklist(column_name, ['"', '`'], '') }}, '{{ delimiter }}')[ORDINAL( {{ ordinal }} )]
{% endmacro %}