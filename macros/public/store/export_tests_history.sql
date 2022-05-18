{% macro export_tests_history(start_date, end_date, tests_history_path=None) %}
    {% set tests_history_query %}
        select
            table_name,
            column_name,
            test_name,
            run_at,
            status,
            execution_time, 
            message, 
            failures_count, 
            failures_json, 
            failures_table,
            severity, 
            compiled_sql
        from
            {{ ref('re_data_test_history') }}
        where date(run_at) between '{{start_date}}' and '{{end_date}}' 
    {% endset %}

    {% set query_result = run_query(tests_history_query) %}
    {% set tests_history_file_path = tests_history_path or 'target/re_data/tests_history.json' %}
    {% do query_result.to_json(tests_history_file_path) %}

{% endmacro %}