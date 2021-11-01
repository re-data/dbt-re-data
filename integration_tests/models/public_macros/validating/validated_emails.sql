with 
    all_emails as (
        select * from {{ ref('validate_emails') }}
    )

select *, case when {{ re_data.valid_email('email') }} then 1 else 0 end as email_valid
    from all_emails 
