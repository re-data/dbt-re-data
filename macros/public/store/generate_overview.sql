{% macro generate_overview(start_date, end_date, interval) %}
-- depends_on: {{ ref('re_data_alerting') }}
-- depends_on: {{ ref('re_data_base_metrics') }}
-- depends_on: {{ ref('re_data_schema_changes') }}
-- depends_on: {{ ref('re_data_columns') }}

    {# time grain is either days or hour #}
    {% set time_grain, num_str = interval.split(':') %}
    {% set num = num_str | int %}
    {% if time_grain == 'hour' %}
        {% set interval_length_sec = num * 3600 %}
    {% else %}
        {% set interval_length_sec = num * 3600 * 24 %}
    {% endif %}
    {{ log('[re_data] interval length in seconds is ' ~ interval_length_sec, info=True) }}
    {% set dbt_graph = tojson(graph) %}
    {% set overview_query %}
        with schema_changes_casted as (
            select id, table_name, operation, column_name, data_type, {{ bool_to_string('is_nullable') }}, prev_column_name, prev_data_type, {{ bool_to_string('prev_is_nullable') }}, detected_time
            from {{ ref('re_data_schema_changes') }}
        ),
        columns_casted as (
            select id, table_name, column_name, data_type, {{ bool_to_string('is_nullable') }}, {{ bool_to_string('is_datetime') }}, computed_on
            from {{ ref('re_data_columns') }} 
        )
        
    (    
        select
            'metric' as type,
            table_name as table_name,
            column_name as column_name,
            computed_on as computed_on,
            {{ to_single_json(['metric', 'value', 'time_window_end', 'interval_length_sec']) }} as data
        from
            {{ ref('re_data_base_metrics') }}
            where date(time_window_end) between '{{start_date}}' and '{{end_date}}'
            and interval_length_sec = {{interval_length_sec}}
    ) union all 
    (
        select
            'alert' as type,
            table_name as table_name,
            column_name as column_name,
            computed_on as computed_on,
            {{ to_single_json(['id', 'metric', 'z_score_value', 'last_value', 'last_avg', 'last_stddev', 'time_window_end', 'interval_length_sec']) }} as data
        from
            {{ ref('re_data_alerting') }}
            where date(time_window_end) between '{{start_date}}' and '{{end_date}}'
            and interval_length_sec = {{interval_length_sec}}
    ) union all
    (
        select
            'schema_change' as type,
            table_name as table_name,
            column_name as column_name,
            detected_time as computed_on,
            {{ to_single_json(['id', 'operation', 'data_type', 'is_nullable', 'prev_column_name', 'prev_data_type', 'prev_is_nullable', 'detected_time']) }} as data
        from
            schema_changes_casted
    ) union all
    (
        select
            'schema' as type,
            table_name as table_name,
            column_name as column_name,
            computed_on as computed_on,
            {{ to_single_json(['data_type', 'is_nullable', 'is_datetime']) }} as data
        from
            columns_casted
    ) union all
    (
        select
            'dbt_graph' as type,
            null as table_name,
            null as column_name,
            {{- dbt_utils.current_timestamp_in_utc() -}} as computed_on,
            {{ quote_text(dbt_graph)}} as data
    ) order by computed_on desc
    {% endset %}

    {% set overview_result = run_query(overview_query) %}
    {% do overview_result.to_json('target/re_data/overview.json') %}
{% endmacro %}