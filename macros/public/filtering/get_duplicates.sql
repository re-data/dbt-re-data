{% https://github.com/re-data/re-data/issues/143 %}

{#
macro returns rows with she same key set (unique_cols)

along with the fields of the base model duplicates information added:
re_data_duplicates_count - total number of duplicates with the same current key set
re_data_duplicate_row_number - number of a duplicate row inside the group of duplicates with the same current key set
#}

{% macro filter_get_duplicates(relation, unique_cols, sort_columns) %}
    (
        with with_row_num_count as (
            select *
            , count(*) over (
            partition by {{ re_data.comma_delimited_list(unique_cols) }} 
            ) as re_data_duplicates_group_rows_count
            , row_number() over (
                partition by {{ re_data.comma_delimited_list(unique_cols) }} {% if sort_columns %} order by {{ re_data.comma_delimited_list(sort_columns) }} {% endif %}
            ) as re_data_duplicates_group_row_number

           
            from {{ relation }}
        ),
        duplicate_rows as (
            select * from with_row_num_count where re_data_duplicates_group_rows_count > 1
        )
        {# return surrogate key as well? #}
        select *
        from duplicate_rows
    ) 
{% endmacro %}