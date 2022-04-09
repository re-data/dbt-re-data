
select
    {{ clean_table_name('table_name') }} as table_name,
    {{ clean_column_name('column_name') }} as column_name,
    right(test_name, 15) as test_name,
    status,
    {{ clean_column_name('message') }} as message,
    cast (failures_count as integer) as failures_count,
    severity
from {{ ref('re_data_test_history') }}