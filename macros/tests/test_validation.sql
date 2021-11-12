
{% test valid_string(model, column_name, macro, where_cond="1=1") %}

    {% set metric_macro = context['re_data'][macro] %}

    select * from {{ model }} where
        not {{ re_data.valid_email(column_name) }}
        and {{ where_cond }}
{% endtest %}