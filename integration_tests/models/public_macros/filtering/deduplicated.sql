with x as 
    {{ re_data.filter_remove_duplicates(
        ref('duplicated'), ['transaction_id'], ['creation_time']) }}

select *, 'take_first' as use_case from x

union all

select *, 'take_last' as use_case from {{ re_data.filter_remove_duplicates(
        ref('duplicated'), ['transaction_id'], ['creation_time desc']) }} duplicates


union all

select *, 'take_all_statuses' as use_case from {{ re_data.filter_remove_duplicates(
    ref('duplicated'), ['transaction_id', 'status'], ['creation_time desc']) }} duplicates