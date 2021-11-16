


{% macro filter_remove_duplicates(relation, unique_cols, sort_columns) %}
    (
        with with_row_num as (
            {{re_data.add_duplication_context(relation, unique_cols, sort_columns)}}
        ),
        one_row_num as (
            select * from with_row_num where re_data_duplicate_group_row_number = 1
        )
        select {{ dbt_utils.star(from=relation) }}
        from one_row_num
    ) 
{% endmacro %}