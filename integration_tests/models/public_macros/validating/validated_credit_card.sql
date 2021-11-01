with 
    all_rows as (
        select * from {{ ref('validate_credit_card') }}
    )

select *, 
    case when {{ re_data.valid_credit_card('credit_card_number') }} then 1 else 0 end as valid_credit_card
    from all_rows 
