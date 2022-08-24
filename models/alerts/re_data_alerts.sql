select
    'anomaly' as type,
    {{ re_data.clean_blacklist('table_name', ['"', '`'], '') }} as model,
    message,
    last_value_text as value,
    time_window_end
from {{ ref('re_data_anomalies') }}
qualify row_number() over (partition by model order by time_window_end desc) = 1
-- this qualify gets the latest alert for each model.

union all

select
    'schema_change' as type,
    {{ re_data.clean_blacklist('table_name', ['"', '`'], '') }} as model,
    {{ generate_schema_change_message('operation', 'column_name', 'prev_column_name', 'prev_data_type', 'data_type', 'detected_time') }} as message,
    '' as value,
    detected_time as time_window_end
from {{ ref('re_data_schema_changes') }}
qualify row_number() over (partition by model order by time_window_end desc) = 1
-- this qualify gets the latest alert for each model.

union all

select 
    type,
    model,
    message,
    value,
    time_window_end
from (
        select
            'test' as type,
            table_name as model,
            {{ generate_failed_test_message('test_name', 'column_name') }} as message,
            status as value,
            run_at as time_window_end
        from {{ ref('re_data_test_history') }}
        qualify row_number() over (partition by model order by time_window_end desc) = 1
        -- this qualify gets the latest alert for each model.
     )
where status = 'Fail' or status = 'Error'
