select
    {{ re_data.clean_capitalize_words(re_data.clean_additional_whitespaces('full_name')) }} as full_name,
    {{ re_data.clean_blacklist('email', ['^[a-zA-Z0-9_.+-]+'], '*****') }} as email
from {{ ref('sample_user_data') }}
