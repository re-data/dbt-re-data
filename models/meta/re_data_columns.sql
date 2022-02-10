{{
    config(
        materialized='table',
    )
}}

-- depends_on: {{ ref('re_data_run_started_at') }}
-- depends_on: {{ ref('re_data_monitored') }}

{% if execute %}
    {% set schemas = run_query(re_data.get_schemas()) %}
    {% if schemas %}

    with columns_from_select as (
        {% for row in schemas %}
            {% set schema_name = re_data.name_in_db(re_data.row_value(row, 'schema')) %}
            {{ get_monitored_columns(schema_name, re_data.row_value(row, 'database')) }}
        {%- if not loop.last %} union all {%- endif %}
        {% endfor %}
    )

    select
        cast (table_name as {{ string_type() }} ) as name,
        cast (table_schema as {{ string_type() }} ) as schema,
        cast (table_catalog as {{ string_type() }} ) as database,
        cast (column_name as {{ string_type() }} ) as column_name,
        cast (data_type as {{ string_type() }} ) as data_type,
        cast (case is_nullable when 'YES' then 1 else 0 end as {{ boolean_type() }} ) as is_nullable,
        {{- dbt_utils.current_timestamp_in_utc() -}} as computed_on
    from columns_from_select

    {% else %}
        {{ empty_columns_table() }}
    {% endif %}

{% else %}
    {{ empty_columns_table() }}
{% endif %}