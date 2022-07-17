
select 
    {{ clean_table_name('table_name') }} as table_name,
    length(sample_data) as sample_data_length
from {{ ref('re_data_table_samples') }}