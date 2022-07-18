
select 
    {{ clean_table_name('table_name') }} as table_name,
    length(sample_data) as sample_data_length
from {{ ref('re_data_table_samples') }}
where {{ clean_table_name('table_name') }} != 'SAMPLE_WITHOUT_TIME_FILTER'

-- SAMPLE_WITHOUT_TIME_FILTER because this table doesn't have a time filter, it's not possible to say how
-- exactly the sampel of it should look like.