{% macro export_table_samples(start_date, end_date, table_samples_path=None) %}
    {% set table_samples_query %}
        select
            table_name,
            sample_data,
            sampled_on
        from
            {{ ref('re_data_table_samples') }}
    {% endset %}

    {% set query_result = run_query(table_samples_query) %}
    {% set table_samples_file_path = table_samples_path or 'target/re_data/table_samples.json' %}
    {% do query_result.to_json(table_samples_file_path) %}

{% endmacro %}