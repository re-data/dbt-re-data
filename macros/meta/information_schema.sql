{% macro get_monitored_columns(for_schema) %}
    {% set splitted = for_schema.split('.') %}
    {% set splitted_length = splitted | length %}
    {% if splitted_length == 2 %}
        {{ adapter.dispatch('get_monitored_columns')(splitted[1], splitted[0]) }}
    {% else %}
        {{ adapter.dispatch('get_monitored_columns')(splitted[0], None) }}
    {% endif %}
{% endmacro %}

{% macro default__get_monitored_columns(table_schema, db_name) %}
    select
        {{full_table_name(table_name, table_schema, table_catalog) }} as table_name,
        column_name,
        data_type,
        is_nullable,
        {{- is_datetime('data_type') -}} as is_datetime,
        {{- time_filter('column_name', 'data_type') -}} as time_filter
    from
    {{ tables_in_schema(table_schema, db_name) }}
{% endmacro %}

{% macro redshift__get_monitored_columns(table_schema, db_name) %}

    {%- call statement('columns', fetch_result=True) -%}
    select
        {{full_table_name(table_name, table_schema, table_catalog) }} as table_name,
        column_name,
        data_type,
        is_nullable,
        {{- is_datetime('data_type')}} as is_datetime,
        {{- time_filter('column_name', 'data_type') -}} as time_filter
    from 
        {% if db_name %}{{db_name}}.{% endif %}information_schema.columns
    where
        table_schema = '{{ table_schema }}'
    {% endcall %}

    {%- set columns = load_result('columns')['data'] -%}

    {% for query_row in columns %}
        select 
            '{{query_row[0]}}'::text as table_name,
            '{{query_row[1]}}'::text as column_name,
            '{{query_row[2]}}'::text as data_type,
            '{{query_row[3]}}' as is_nullable,
            {{query_row[4]}} as is_datetime,
            {% if query_row[5] %} '{{query_row[5]}}'::text {% else %} null {% endif %} as time_filter
    {%- if not loop.last %} union all {%- endif %}
    {% endfor %}

{% endmacro %}

{% macro tables_in_schema(schema_name, db_name) %}
    {{ adapter.dispatch('tables_in_schema')(schema_name, db_name) }}
{% endmacro %}

{% macro default__tables_in_schema(schema_name, db_name) %}
    {% if db_name %} {{ db_name}} . {% endif %} INFORMATION_SCHEMA.columns
    where
    table_schema = '{{ schema_name }}'
{% endmacro %}

{% macro bigquery__tables_in_schema(schema_name, db_name) %}
    {% if db_name %} `{{ db_name}}` . {% endif %} `{{schema_name}}` . `INFORMATION_SCHEMA`.`COLUMNS`
{% endmacro %}