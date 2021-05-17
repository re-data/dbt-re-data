{% macro get_monitored_columns(for_schema) %}
    {{ adapter.dispatch('get_monitored_columns')(for_schema) }}
{% endmacro %}

{% macro default__get_monitored_columns(for_schema) %}
    select
        {{table_name() }} as table_name,
        column_name,
        data_type,
        is_nullable,
        {{- is_datetime('data_type') -}} as is_datetime,
        {{- time_filter('column_name', 'data_type') -}} as time_filter
    from
    {{ tables_in_schema(for_schema) }}
{% endmacro %}

{% macro redshift__get_monitored_columns(for_schema) %}

    {%- call statement('columns', fetch_result=True) -%}
    select
        {{table_name() }} as table_name,
        "column" as column_name,
        type as data_type,
        not "notnull" as is_nullable,
        {{- is_datetime('data_type')}} as is_datetime,
        {{- time_filter('column', 'type') -}} as time_filter
    from 
        pg_catalog.PG_TABLE_DEF
    where
        schemaname = '{{ for_schema }}'
    {% endcall %}

    {%- set columns = load_result('columns')['data'] -%}

    {% for query_row in columns %}
        select 
            '{{query_row[0]}}'::text as table_name,
            '{{query_row[1]}}'::text as column_name,
            '{{query_row[2]}}'::text as data_type,
            {{query_row[3]}} as is_nullable,
            {{query_row[4]}} as is_datetime,
            {% if query_row[5] %} '{{query_row[5]}}'::text {% else %} null {% endif %} as time_filter
    {%- if not loop.last %} union all {%- endif %}
    {% endfor %}

{% endmacro %}

{% macro tables_in_schema(for_schema) %}
    {{ adapter.dispatch('tables_in_schema')(for_schema) }}
{% endmacro %}

{% macro default__tables_in_schema(for_schema) %}
    INFORMATION_SCHEMA.columns
    where
    table_schema = '{{ for_schema }}'
{% endmacro %}

{% macro bigquery__tables_in_schema(for_schema) %}
    `{{for_schema}}` . `INFORMATION_SCHEMA`.`COLUMNS`
{% endmacro %}