

{% macro valid_regex(column_name, to_validate) %}
{% set pattern = re_data.get_regex_for(to_validate) %}
  case when 
    {{ column_name }} is null then false 
    else {{ re_data.regex_match_expression(column_name, pattern) }}
  end
{% endmacro %}

{% macro valid_email(column_name) %}
    {{ re_data.valid_regex(column_name, 'email')}}
{% endmacro %}

{% macro valid_date_eu(column_name) %}
    {{ re_data.valid_regex(column_name, 'date_eu')}}
{% endmacro %}

{% macro valid_date_us(column_name) %}
    {{ re_data.valid_regex(column_name, 'date_us')}}
{% endmacro %}

{% macro valid_date_inverse(column_name) %}
    {{ re_data.valid_regex(column_name, 'date_inverse')}}
{% endmacro %}

{% macro valid_date_iso_8601(column_name) %}
    {{ re_data.valid_regex(column_name, 'date_iso_8601')}}
{% endmacro %}

{% macro valid_time_24h(column_name) %}
    {{ re_data.valid_regex(column_name, 'time_24h')}}
{% endmacro %}

{% macro valid_time_12h(column_name) %}
    {{ re_data.valid_regex(column_name, 'time_12h')}}
{% endmacro %}

{% macro valid_time(column_name) %}
    {{ re_data.valid_regex(column_name, 'time')}}
{% endmacro %}

{% macro valid_ip_v4(column_name) %}
    {{ re_data.valid_regex(column_name, 'ipv4_address')}}
{% endmacro %}

{% macro valid_ip_v6(column_name) %}
    {{ re_data.valid_regex(column_name, 'ipv6_address')}}
{% endmacro %}

{% macro valid_ip(column_name) %}
    (
        {{ re_data.valid_regex(column_name, 'ipv4_address')}}
    or 
        {{ re_data.valid_regex(column_name, 'ipv6_address')}}
    )
{% endmacro %}

{% macro valid_number(column_name) %}
    {{ re_data.valid_regex(column_name, 'number_whole')}}
{% endmacro %}

{% macro valid_number_decimal_point(column_name) %}
    {{ re_data.valid_regex(column_name, 'number_decimal_point')}}
{% endmacro %}

{% macro valid_number_decimal_comma(column_name) %}
    {{ re_data.valid_regex(column_name, 'number_decimal_comma')}}
{% endmacro %}

{% macro valid_number_percentage(column_name) %}
    {{ re_data.valid_regex(column_name, 'number_percentage')}}
{% endmacro %}

{% macro valid_number_percentage_point(column_name) %}
    {{ re_data.valid_regex(column_name, 'number_percentage_point')}}
{% endmacro %}

{% macro valid_number_percentage_comma(column_name) %}
    {{ re_data.valid_regex(column_name, 'number_percentage_comma')}}
{% endmacro %}

{% macro valid_phone(column_name) %}
    {{ re_data.valid_regex(column_name, 'phone')}}
{% endmacro %}

{% macro valid_uuid(column_name) %}
    {{ re_data.valid_regex(column_name, 'uuid')}}
{% endmacro %}

{% macro valid_credit_card(column_name) %}
    {{ re_data.valid_regex(column_name, 'credit_card_number')}}
{% endmacro %}