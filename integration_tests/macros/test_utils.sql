
{% macro to_big_integer(field) %}
    cast (round({{field}} * 1000) as integer) as {{field}}
{% endmacro %}

{% macro clean_table_name(field) %}
    upper(
        {{-
            re_data.clean_blacklist(
                re_data.split_and_return_nth_value(field, '.', 3),
                ['"', '`'],
                ''
            )
        -}}
    )
{% endmacro %}

{% macro clean_column_name(field) %}
    case when ({{ field }} = '' or  {{ field }} is null ) then '---' else  upper({{field}}) end
{% endmacro %}
