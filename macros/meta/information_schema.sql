{% macro get_monitored_columns(schema, database) %}
    {{ adapter.dispatch('get_monitored_columns', 're_data')(schema, database) }}
{% endmacro %}

{% macro default__get_monitored_columns(table_schema, db_name) %}
    {% set relation = api.Relation.create(database=db_name, schema=table_schema) %}
    select
        table_name,
        table_schema,
        table_catalog,
        column_name,
        data_type,
        is_nullable
    from
        {{ relation.information_schema('COLUMNS') }}
    where
        table_schema = '{{ table_schema }}'
{% endmacro %}

{% macro redshift__get_monitored_columns(table_schema, db_name) %}
    select
        table_name,
        table_schema,
        table_catalog,
        column_name,
        data_type,
        is_nullable
    from
        svv_columns
    where
        table_schema = '{{ table_schema }}'
{% endmacro %}
