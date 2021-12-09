{% macro generate_overview() %}
    {% set overview_query %}
        select 
            anomalies,
            metrics,
            schema_changes,
            table_schema,
            graph,
            generated_at
        from {{ ref('re_data_overview') }}
        order by generated_at desc 
        limit 1
    {% endset %}

    {% set overview_result = run_query(overview_query) %}
    {% do overview_result.to_json('target/re_data/overview.json') %}
{% endmacro %}