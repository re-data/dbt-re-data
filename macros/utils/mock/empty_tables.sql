{# /* Quite hacky macros to create empty tables in case nothing is yet to be monitoed */ #}

{% macro all_types_select() %}
    with types_table as (
        select
            cast (null as {{ string_type() }}) as string_type,
            cast (null as {{ numeric_type() }}) as numeric_type,
            cast (null as {{ timestamp_type() }}) as timestamp_type,
            cast (null as {{ boolean_type() }}) as boolean_type
    )
{% endmacro %}

{% macro dummy_empty_schema_changes_table() %}
    {{ re_data.all_types_select() }}
    select 
        cast (string_type as {{ string_type() }} ) as id,
        cast (string_type as {{ string_type() }} ) as table_name,
        cast (string_type as {{ string_type() }} ) as operation,
        cast (string_type as {{ string_type() }} ) as column_name,
        cast (string_type as {{ string_type() }} ) as data_type,
        cast (boolean_type as {{ boolean_type() }} ) as is_nullable,
        cast (string_type as {{ string_type() }} ) as prev_column_name,
        cast (string_type as {{ string_type() }} ) as prev_data_type,
        cast (boolean_type as {{ boolean_type() }} ) as prev_is_nullable,
        cast (timestamp_type as {{ timestamp_type() }} ) as detected_time 
    from types_table
    where numeric_type = 2

{% endmacro %}

{% macro empty_last_base_metrics() %}
    {{ re_data.all_types_select() }}
    select 
        cast (string_type as {{ string_type() }} ) as table_name,
        cast (string_type as {{ string_type() }} ) as column_name,
        cast (string_type as {{ string_type() }} ) as metric,
        cast (numeric_type as {{ numeric_type() }} ) as value
    from types_table
    where numeric_type = 2
{% endmacro %}

{% macro empty_code_monitored() %}
    {{ re_data.all_types_select() }}
    select 
        cast (string_type as {{ string_type() }} ) as name,
        cast (string_type as {{ string_type() }} ) as schema,
        cast (string_type as {{ string_type() }} ) as database,
        cast (string_type as {{ string_type() }} ) as time_filter,
        cast (string_type as {{ string_type() }} ) as metrics,
        cast (string_type as {{ string_type() }} ) as columns,
        cast (string_type as {{ string_type() }} ) as anomaly_detector
    from types_table
    where numeric_type = 2
{% endmacro %}

{% macro empty_columns_table() %}
    {{ re_data.all_types_select() }}
    select 
        cast (string_type as {{ string_type() }} ) as name,
        cast (string_type as {{ string_type() }} ) as schema,
        cast (string_type as {{ string_type() }} ) as database,
        cast (string_type as {{ string_type() }} ) as column_name,
        cast (string_type as {{ string_type() }} ) as data_type,
        cast (boolean_type as {{ boolean_type() }} ) as is_nullable,
        cast (string_type as {{ string_type() }} ) as time_filter,
        cast (timestamp_type as {{ timestamp_type() }} ) as computed_on
    from types_table
    where numeric_type = 2
{% endmacro %}

{% macro empty_overview_table() %}
    {{ re_data.all_types_select() }}
    select 
        cast (string_type as {{ string_type() }} ) as anomalies,
        cast (string_type as {{ string_type() }} ) as metrics,
        cast (string_type as {{ string_type() }} ) as schema_changes,
        cast (string_type as {{ string_type() }} ) as table_schema
        cast (string_type as {{ string_type() }} ) as graph,
        cast (timestamp_type as {{ timestamp_type() }} ) as generated_at
{% endmacro %}

{% macro empty_table_generic(list) %}
    {{ re_data.all_types_select() }}
    select
    {% for name, type in list %}
         {{ type }}_type as {{ name }}
        {%- if not loop.last %}, {%- endif %}
    {% endfor %}
    from types_table
    where string_type is not null
{% endmacro %}