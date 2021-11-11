{% macro add_duplication_context(relation, unique_cols, sort_columns) %}

            select {{ dbt_utils.star(from=relation) }}
            , count(*) over (
                 partition by {{ re_data.comma_delimited_list(unique_cols) }} 
            ) as re_data_duplicates_group_rows_count
            , row_number() over (
                partition by {{ re_data.comma_delimited_list(unique_cols) }} {% if sort_columns %} order by {{ re_data.comma_delimited_list(sort_columns) }} {% endif %}
            ) as re_data_duplicates_group_row_number

            from {{ relation }}

{% endmacro %}