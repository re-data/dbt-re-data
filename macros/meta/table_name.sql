{% macro full_table_name(table_name, table_schema, table_catalog) %}
    {{ adapter.dispatch('full_table_name')(table_name, table_schema, table_catalog) }}
{% endmacro %}


{% macro default__full_table_name(table_name, table_schema, table_catalog) %}
    '"' || table_catalog || '"' || '.' || '"' || table_schema || '"' || '.' || '"' || table_name || '"'
{% endmacro %}


{% macro bigquery__full_table_name(table_name, table_schema, table_catalog) %}
    '`' || table_catalog || '`' || '.' || '`' || table_schema || '`' || '.' || '`' || table_name || '`'
{% endmacro %}


{% macro full_table_name_values(table_name, table_schema, table_catalog) %}
    {{ adapter.dispatch('full_table_name_values')(table_name, table_schema, table_catalog) }}
{% endmacro %}


{% macro default__full_table_name_values(table_name, table_schema, table_catalog) %}
    '"{{table_catalog}}"."{{table_schema}}"."{{table_name}}"'
{% endmacro %}


{% macro bigquery__full_table_name_values(table_name, table_schema, table_catalog) %}
    '`{{table_catalog}}`.`{{table_schema}}`.`{{table_name}}`'
{% endmacro %}