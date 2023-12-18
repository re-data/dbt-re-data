{% macro get_column_specific_anomaly(
    database, schema, column_name, table_name, metric_name
) %}
    {% set column_path = "column." ~ column_name %}
    select
        name as table_name,
        schema as schema,
        database as database,
        {{ "'" ~ column_name ~ "'" }} as column_name,
        {{ "'" ~ metric_name ~ "'" }} as metric_name,
        json_query(t, {{ "'$." ~ metric_name ~ "'" }}) as metric_spec
    from
        {{ ref("re_data_selected") }},
        unnest({{ re_data.json_extract_array("additional_metrics", column_path) }}) t
    where
        name = {{ "'" ~ table_name ~ "'" }}
        and schema = {{ "'" ~ schema ~ "'" }}
        and database = {{ "'" ~ database ~ "'" }}

{% endmacro %}
