{% macro full_table_name(table_name, table_schema, table_catalog) %}
    {{ adapter.dispatch('full_table_name', 're_data')(table_name, table_schema, table_catalog) }}
{% endmacro %}


{% macro default__full_table_name(table_name, table_schema, table_catalog) %}
    '"' || {{table_catalog}} || '"' || '.' || '"' || {{table_schema}} || '"' || '.' || '"' || {{table_name}} || '"'
{% endmacro %}


{% macro bigquery__full_table_name(table_name, table_schema, table_catalog) %}
    '`' || {{table_catalog}} || '`' || '.' || '`' || {{table_schema}} || '`' || '.' || '`' || {{table_name}} || '`'
{% endmacro %}


{% macro full_table_name_values(table_name, table_schema, table_catalog) %}
    {% set result = adapter.dispatch('full_table_name_values', 're_data')(table_name, table_schema, table_catalog) %}
    {{ return (result.strip()) }}
{% endmacro %}

{% macro default__full_table_name_values(table_name, table_schema, table_catalog) %}
    "{{table_catalog}}"."{{table_schema}}"."{{table_name}}"
{% endmacro %}


{% macro bigquery__full_table_name_values(table_name, table_schema, table_catalog) %}
    `{{table_catalog}}`.`{{table_schema}}`.`{{table_name}}`
{% endmacro %}


{% macro snowflake__full_table_name_values(table_name, table_schema, table_catalog) %}
    "{{table_catalog|upper}}"."{{table_schema|upper}}"."{{table_name|upper}}"
{% endmacro %}