
{% macro comma_delimited_list(args) %}
    {%- for arg in args %} 
        {{- arg -}} {{- ", " if not loop.last else "" -}}
    {% endfor %}
{% endmacro %}}

{% macro filter_remove_duplicates(relation, unique_cols, sort_columns) %}
    (
        with with_row_num as (
            select *, row_number() over (
                partition by {{ re_data.comma_delimited_list(unique_cols) }} {% if sort_columns %} order by {{ re_data.comma_delimited_list(sort_columns) }} {% endif %}
            ) as re_data_row_num
            from {{ relation }}
        ),
        one_row_num as (
            select * from with_row_num where re_data_row_num = 1
        )
        select {{ dbt_utils.star(from=relation) }}
        from one_row_num
    ) 
{% endmacro %}