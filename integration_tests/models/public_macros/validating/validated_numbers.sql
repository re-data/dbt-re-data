with 
    all_num_rows as (
        select * from {{ ref('validate_numbers') }}
    )

select *, 
    case when {{ re_data.valid_number('number') }} then 1 else 0 end as is_number,
    case when {{ re_data.valid_number_decimal_point('number') }} then 1 else 0 end as is_number_decimal_point,
    case when {{ re_data.valid_number_decimal_comma('number') }} then 1 else 0 end as is_number_decimal_comma,
    case when {{ re_data.valid_number_percentage('number') }} then 1 else 0 end as is_percentage,
    case when {{ re_data.valid_number_percentage_point('number') }} then 1 else 0 end as is_percentage_decimal_point,
    case when {{ re_data.valid_number_percentage_comma('number') }} then 1 else 0 end as is_percentage_decimal_comma
    from all_num_rows 
