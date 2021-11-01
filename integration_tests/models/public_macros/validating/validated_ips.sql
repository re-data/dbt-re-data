with 
    all_rows as (
        select * from {{ ref('validate_ip') }}
    )

select *, 
    case when {{ re_data.valid_ip_v4('ip_address') }} then 1 else 0 end as valid_ip_v4,
    case when {{ re_data.valid_ip_v6('ip_address') }} then 1 else 0 end as valid_ip_v6,
    case when {{ re_data.valid_ip('ip_address') }} then 1 else 0 end as valid_ip
    from all_rows 
