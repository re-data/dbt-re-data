{%- macro order_by_if_time_filter(time_filter) -%}
    {%- if time_filter is not none -%}
        order by {{ time_filter }} desc
    {%- endif -%}
{%- endmacro -%}


{% macro re_data_last_table_samples() %}
    {{ re_data.generate_depends(['re_data_selected', 're_data_monitored', 're_data_columns', 're_data_run_started_at', 're_data_last_table_samples_part']) }}

    {{
        config(
            materialized='table',
        )
    }}

    {% if var.has_var('re_data:store_table_samples') %}
        {% set store_samples = var('re_data:store_table_samples') %}
    {% endif %}
    {% if not re_data.in_compile() and store_samples is sameas true %}
        {%- set tables = run_query(re_data.get_tables()) %}

        {% set samples_list = [] %}
        {%- for sample_table in tables %}
            {% set name = re_data.row_value(sample_table, 'name') %}
            {% set schema = re_data.row_value(sample_table, 'schema') %}
            {% set database = re_data.row_value(sample_table, 'database') %}
            {% set time_filter = re_data.row_value(sample_table, 'time_filter') %}    
            {% set table_name = (database + '.' + schema + '.' + name) | lower %}

            {% set samples_query %}
                select * from {{ table_name }}
                {{ order_by_if_time_filter(time_filter) }}
                limit 10
            {% endset %}

            {% set samples = re_data.agate_to_list(run_query(samples_query)) %}
            {% do samples_list.append({
                'table_name': table_name,
                'sample_data': samples,
            }) %}

        {% endfor %}
        {% do re_data.insert_list_to_table(
                ref('re_data_last_table_samples_part'),
                samples_list,
                ['table_name', 'sample_data']
            ) %}
    {% endif %}

    {{ re_data.empty_last_table_samples() }}

{% endmacro %}