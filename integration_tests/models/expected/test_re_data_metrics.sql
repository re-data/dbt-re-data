{% set table_name = re_data.full_table_name_values(
    "sample_table", target.schema + "_raw", re_data.get_target_database()) %}

select * from {{ ref('re_data_metrics')}}
where table_name = '{{ table_name }}'

