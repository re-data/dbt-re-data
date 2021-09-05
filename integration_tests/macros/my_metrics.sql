{% macro diff(column_name) %}
    max({{column_name}}) - min({{column_name}})
{% endmacro %}


{% macro buy_count(time_column) %}
    coalesce(
        sum(
            case when event_type = 'buy'
                then 1
            else 0
            end
        ), 0
    )
{% endmacro %}
