with 
    all_rows as (
        select * from {{ ref('validate_uuid') }}
    )

select *, 
    case when {{ re_data.valid_uuid('uuid') }} then 1 else 0 end as valid_uuid
    from all_rows 
