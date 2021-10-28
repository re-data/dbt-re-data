with 
    all_emails as (
        select * from {{ ref('validate_emails_sample') }}
    )

select *, {{ re_data.valid_email('email') }} as email_valid
    from all_emails 
