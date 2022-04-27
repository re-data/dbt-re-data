
{% macro overview_select_base(type, timestamp_col) %}
    '{{ type }}' as {{ re_data.quote_column('type') }},
    table_name as {{ re_data.quote_column('table_name') }},
    column_name as {{ re_data.quote_column('column_name') }},
    {{ timestamp_col }} as {{ re_data.quote_column('computed_on') }},
{% endmacro %}

{% macro generate_overview(start_date, end_date, interval, overview_path=None) %}
-- depends_on: {{ ref('re_data_anomalies') }}
-- depends_on: {{ ref('re_data_base_metrics') }}
-- depends_on: {{ ref('re_data_schema_changes') }}
-- depends_on: {{ ref('re_data_columns') }}

    {# time grain is either days or hour #}
    {% set time_grain, num_str = interval.split(':') %}
    {% set num = num_str | int %}
    {% if time_grain == 'hours' %}
        {% set interval_length_sec = num * 3600 %}
    {% elif time_grain == 'days'%}
        {% set interval_length_sec = num * 3600 * 24 %}
    {% else %}
        {{ exceptions.raise_compiler_error("Invalid interval. Got: " ~ interval) }}
    {% endif %}
    {{ dbt_utils.log_info('[re_data] interval length in seconds is ' ~ interval_length_sec) }}
    {% set overview_query %}
        with schema_changes_casted as (
            select id, table_name, operation, column_name, data_type, {{ bool_to_string('is_nullable') }}, prev_column_name, prev_data_type, {{ bool_to_string('prev_is_nullable') }}, detected_time
            from {{ ref('re_data_schema_changes') }}
        ),
        columns_casted as (
            select {{ full_table_name('name', 'schema', 'database') }} as table_name, column_name, data_type, {{ bool_to_string('is_nullable') }}, computed_on
            from {{ ref('re_data_columns') }} 
        )
        
    (    
        select
            {{ overview_select_base('metric', 'computed_on')}}
            {{ to_single_json(['metric', 'value', 'time_window_end', 'interval_length_sec']) }} as {{ re_data.quote_column('data') }}
        from
            {{ ref('re_data_base_metrics') }}
            where date(time_window_end) between '{{start_date}}' and '{{end_date}}'
            and interval_length_sec = {{interval_length_sec}}
    ) union all 
    (
        select
            {{ overview_select_base('anomaly', 'computed_on')}}
            {{ to_single_json(['id', 'metric', 'z_score_value', 'last_value', 'last_avg', 'last_stddev', 'time_window_end', 'interval_length_sec']) }} as {{ re_data.quote_column('data') }}
        from
            {{ ref('re_data_anomalies') }}
            where date(time_window_end) between '{{start_date}}' and '{{end_date}}'
            and interval_length_sec = {{interval_length_sec}}
    ) union all
    (
        select
            {{ overview_select_base('schema_change', 'detected_time')}}
            {{ to_single_json(['id', 'operation', 'data_type', 'is_nullable', 'prev_column_name', 'prev_data_type', 'prev_is_nullable', 'detected_time']) }} as {{ re_data.quote_column('data') }}
        from
            schema_changes_casted
            where date(detected_time) >= '{{start_date}}'
    ) union all
    (
        select
            {{ overview_select_base('schema', 'computed_on')}}
            {{ to_single_json(['data_type', 'is_nullable']) }} as {{ re_data.quote_column('data') }}
        from
            columns_casted
    )
    union all 
    (
        select 
            'alert' as {{ re_data.quote_column('type') }},
            model as {{ re_data.quote_column('table_name') }},
            null as {{ re_data.quote_column('column_name') }},
            time_window_end as {{ re_data.quote_column('computed_on') }},
            {{ to_single_json(['type', 'model', 'message', 'value', 'time_window_end']) }} as {{ re_data.quote_column('data') }}
        from
            {{ ref('re_data_alerts') }}
        where
            case
                when type = 'anomaly' then time_window_end between '{{ start_date }}' and '{{ end_date }}'
                else time_window_end >= '{{ start_date }}'
            end
    ) union all
    (
        select
            {{ overview_select_base('test', 'run_at')}}
            {{ to_single_json([
                'status', 'test_name', 'run_at', 'execution_time', 'message', 'failures_count', 'failures_json', 'failures_table', 'severity', 'compiled_sql'
            ]) }} as {{ re_data.quote_column('data') }}
        from
            {{ ref('re_data_test_history') }}
        where date(run_at) between '{{start_date}}' and '{{end_date}}' 
    )
    order by {{ re_data.quote_column('computed_on')}} desc
    {% endset %}

    {% set overview_result = run_query(overview_query) %}
    {% set overview_file_path = overview_path or 'target/re_data/overview.json' %}
    {% do overview_result.to_json(overview_file_path) %}
{% endmacro %}