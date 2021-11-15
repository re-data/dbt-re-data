{% https://github.com/re-data/re-data/issues/143 %}

{#
macro returns rows with she same key set (unique_cols)

along with the fields of the base model duplicates information added:
re_data_duplicate_count - total number of duplicates with the same current key set
re_data_duplicate_row_number - number of a duplicate row inside the group of duplicates with the same current key set
#}

{% macro filter_get_duplicates(relation, unique_cols, sort_columns) %}
    (
        with duplication_context as (
            {{re_data.add_duplication_context(relation, unique_cols, sort_columns)}}
        ),
        duplicate_rows as (
            select * from duplication_context where re_data_duplicate_group_row_count > 1
        )
        {# return surrogate key as well? #}
        select *
        from duplicate_rows
    ) 
{% endmacro %}