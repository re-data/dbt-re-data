with x as 
    {{ re_data.filter_remove_duplicates(
        ref('duplicated'), ['transaction_id'], ['creation_time']) }}

select * from x
union all

select * from {{ re_data.filter_remove_duplicates(
        ref('duplicated'), ['transaction_id'], ['creation_time desc']) }} duplicates

