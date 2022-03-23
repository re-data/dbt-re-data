{{
    config(
        materialized='view'
    )
}}

select 
    sum(case when status = 'Fail' then 1 else 0 end) as failed,
    sum(case when status = 'Pass' then 1 else 0 end) as passed,
    run_at
from {{ ref ('re_data_test_history') }}
group by run_at
order by run_at desc