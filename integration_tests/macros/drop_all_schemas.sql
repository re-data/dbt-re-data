{% macro drop_all_schemas(schema_name) %}
    {% set schemas_to_drop = [
        schema_name,
        schema_name + '_re',
        schema_name + '_re_internal',
        schema_name + '_raw',
        schema_name + '_expected',
        schema_name + '_dbt_test__audit'
    ] %}
    {% for schema in schemas_to_drop %}
        {% set relation = api.Relation.create(database=target.database, schema=schema) %}
        {% do adapter.drop_schema(relation) %}
    {% endfor %}
{% endmacro %}