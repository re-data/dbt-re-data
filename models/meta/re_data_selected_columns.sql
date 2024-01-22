-- depends_on: {{ ref('re_data_z_score') }}
-- depends_on: {{ ref('re_data_selected') }}
{% set column_metric %}
        select distinct
            {{ split_and_return_nth_value("table_name", ".", 1) }} as database,
            {{ split_and_return_nth_value("table_name", ".", 2) }} as schema,
            {{ split_and_return_nth_value("table_name", ".", 3) }} as name,
            column_name,
            metric
        from {{ ref('re_data_z_score')}}
        where column_name != ''
{% endset %}
{% set column_metric_details = run_query(column_metric) %}
{% for col in column_metric_details %}
    {% set column_name = re_data.row_value(col, "column_name") %}
    {% set metric = re_data.row_value(col, "metric") %}
    {% set database = re_data.row_value(col, "database") %}
    {% set schema = re_data.row_value(col, "schema") %}
    {% set table_name = re_data.row_value(col, "name") %}
    {{
        get_column_specific_anomaly(
            database=database,
            schema=schema,
            column_name=column_name,
            table_name=table_name,
            metric_name=metric,
        )
    }}
    {%- if not loop.last %}
        union all
    {%- endif %}

{% endfor %}
