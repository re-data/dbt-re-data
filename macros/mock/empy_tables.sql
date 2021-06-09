{# /* Quite hacky macros to create empty tables in case nothing is yet to be monitoed */ #}

{% macro dummy_empty_table_generic(has_time_window_start) %}

    with dummy_table as (
        select
            cast (null as {{ string_type() }}) as some_string,
            cast (1 as {{ numeric_type() }}) as some_num,
            cast (null as {{ timestamp_type() }}) as some_time
    )
    select 
        cast (some_string as {{ string_type() }} ) as table_name,
        cast (some_string as {{ string_type() }} ) as column_name,
        cast (some_string as {{ string_type() }} ) as metric,
        cast (some_num as {{ numeric_type() }} ) as value,
        {% if has_time_window_start %}
            cast (some_time as {{ timestamp_type() }} ) as time_window_start,
        {% endif %}
        cast (some_time as {{ timestamp_type() }} ) as time_window_end,
        cast (some_time as {{ timestamp_type() }} ) as computed_on 
    from dummy_table
    where some_num = 2

{% endmacro %}

{% macro dummy_empty_fressness_table() %}
    {{ dummy_empty_table_generic(false)}}
{% endmacro %}


{% macro dummy_empty_base_metrics_table() %}
    {{ dummy_empty_table_generic(true)}}
{% endmacro %}
