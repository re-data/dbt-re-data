{# /* Quite hacky macros to create empty tables in case nothing is yet to be monitoed */ #}

{% macro dummy_to_select() %}
    with dummy_table as (
        select
            cast (null as {{ string_type() }}) as some_string,
            cast (1 as {{ numeric_type() }}) as some_num,
            cast (null as {{ timestamp_type() }}) as some_time,
            cast (true as {{ boolean_type() }}) as some_bool
    )
{% endmacro %}

{% macro dummy_empty_table_generic(has_time_window_start) %}

    {{ re_data.dummy_to_select() }}
    select 
        cast (some_string as {{ string_type() }} ) as id,
        cast (some_string as {{ string_type() }} ) as table_name,
        cast (some_string as {{ string_type() }} ) as column_name,
        cast (some_string as {{ string_type() }} ) as metric,
        cast (some_num as {{ numeric_type() }} ) as value,
        {% if has_time_window_start %}
            cast (some_time as {{ timestamp_type() }} ) as time_window_start,
        {% endif %}
        cast (some_time as {{ timestamp_type() }} ) as time_window_end,
        cast (some_num as {{ integer_type() }}) as interval_length_sec,
        cast (some_time as {{ timestamp_type() }} ) as computed_on 
    from dummy_table
    where some_num = 2

{% endmacro %}

{% macro dummy_empty_base_metrics_table() %}
    {{ re_data.dummy_empty_table_generic(true)}}
{% endmacro %}

{% macro dummy_empty_schema_changes_table() %}
    {{ re_data.dummy_to_select() }}
    select 
        cast (some_string as {{ string_type() }} ) as id,
        cast (some_string as {{ string_type() }} ) as table_name,
        cast (some_string as {{ string_type() }} ) as operation,
        cast (some_string as {{ string_type() }} ) as column_name,
        cast (some_string as {{ string_type() }} ) as data_type,
        cast (some_bool as {{ boolean_type() }} ) as is_nullable,
        cast (some_string as {{ string_type() }} ) as prev_column_name,
        cast (some_string as {{ string_type() }} ) as prev_data_type,
        cast (some_bool as {{ boolean_type() }} ) as prev_is_nullable,
        cast (some_time as {{ timestamp_type() }} ) as detected_time 
    from dummy_table
    where some_num = 2

{% endmacro %}

{% macro empty_last_base_metrics() %}
    {{ re_data.dummy_to_select() }}
    select 
        cast (some_string as {{ string_type() }} ) as table_name,
        cast (some_string as {{ string_type() }} ) as column_name,
        cast (some_string as {{ string_type() }} ) as metric,
        cast (some_num as {{ numeric_type() }} ) as value
    from dummy_table
    where some_num = 2
{% endmacro %}

{% macro empty_code_monitored() %}
    {{ re_data.dummy_to_select() }}
    select 
        cast (some_string as {{ string_type() }} ) as table_name,
        cast (some_string as {{ string_type() }} ) as time_filter,
        cast (some_bool as {{ boolean_type() }} ) as actively_monitored,
        cast (some_string as {{ string_type() }} ) as metrics,
        cast (some_string as {{ string_type() }} ) as columns
    from dummy_table
    where some_num = 2
{% endmacro %}

{% macro empty_columns_table() %}
    {{ re_data.dummy_to_select() }}
    select 
        cast (some_string as {{ string_type() }} ) as table_name,
        cast (some_string as {{ string_type() }} ) as column_name,
        cast (some_string as {{ string_type() }} ) as data_type,
        cast (some_bool as {{ boolean_type() }} ) as is_nullable,
        cast (some_bool as {{ boolean_type() }} ) as is_datetime,
        cast (some_string as {{ string_type() }} ) as time_filter
    from dummy_table
    where some_num = 2
{% endmacro %}

{% macro empty_overview_table() %}
    {{ re_data.dummy_to_select() }}
    select 
        cast (some_string as {{ string_type() }} ) as anomalies,
        cast (some_string as {{ string_type() }} ) as metrics,
        cast (some_string as {{ string_type() }} ) as schema_changes,
        cast (some_string as {{ string_type() }} ) as table_schema
        cast (some_string as {{ string_type() }} ) as graph,
        cast (some_time as {{ timestamp_type() }} ) as generated_at
{% endmacro %}

{% macro empty_test_history() %}
    {{ re_data.dummy_to_select() }}
    select 
        cast (some_time as {{ string_type() }} ) as table_name,
        cast (some_string as {{ string_type() }} ) as column_name,
        cast (some_string as {{ string_type() }} ) as test_name,
        cast (some_string as {{ string_type() }} ) as status,
        cast (some_time as {{ timestamp_type() }} ) as run_at
    from dummy_table
    where some_num = 2
{% endmacro %}