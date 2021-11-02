with 
    all_rows as (
        select * from {{ ref('validate_date_and_time') }}
    )

select *, 
    case when {{ re_data.valid_date_eu('date_time') }} then 1 else 0 end as valid_date_eu,
    case when {{ re_data.valid_date_us('date_time') }} then 1 else 0 end as valid_date_us,
    case when {{ re_data.valid_date_inverse('date_time') }} then 1 else 0 end as valid_date_inverse,
    case when {{ re_data.valid_date_iso_8601('date_time') }} then 1 else 0 end as valid_date_iso_8601,
    case when {{ re_data.valid_time_24h('date_time') }} then 1 else 0 end as valid_time_24h,
    case when {{ re_data.valid_time_12h('date_time') }} then 1 else 0 end as valid_time_12h,
    case when {{ re_data.valid_time('date_time') }} then 1 else 0 end as valid_time
    from all_rows 
