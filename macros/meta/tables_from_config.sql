{% macro get_tables_from_config() %}

    {% set tables_def = [] %}

    {% for group in var('re_data:monitored') %}
        {% for table in group['tables'] %}
            {% do tables_def.extend(re_data.monitoring_spec(table, group)) %}
        {% endfor %}
    {% endfor %}

    {% if tables_def != [] %}
        {{ adapter.dispatch('get_tables_from_config', 're_data')(tables_def) }}
    {% else %}
        {{ re_data.empty_code_monitored() }}
    {% endif %}

{% endmacro %}

{% macro default__get_tables_from_config(tables_def) %}

    select table_name , cast (time_filter as {{ string_type() }}) as time_filter, cast (actively_monitored as {{boolean_type()}}) as actively_monitored, metrics
    from ( 
        values
        {{ tables_from_config_values(tables_def) }}
    ) as monitored(table_name, time_filter, actively_monitored, metrics)

{% endmacro %}

{% macro bigquery__get_tables_from_config(tables_def) %}
    select * from unnest(array<struct<
        table_name {{ string_type() }},
        time_filter {{ string_type() }},
        actively_monitored {{ boolean_type() }},
        metrics {{ string_type() }}
    >> [
        {{ tables_from_config_values(tables_def) }}
    ])
{% endmacro %}

{% macro redshift__get_tables_from_config(tables_def) %}
    {{ tables_from_config_values_from_temp_table(tables_def) }}
{% endmacro %}

{% macro tables_from_config_values_from_temp_table(tables_def) %}
    {% set create_temp_table_query %}
        create temp table re_data_temporary_monitored_table (
            table_name {{ string_type()}},
            time_filter {{ string_type()}},
            actively_monitored {{ boolean_type() }},
            metrics {{ string_type() }}
        );
        insert into re_data_temporary_monitored_table values
        {{ tables_from_config_values(tables_def) }}
    {% endset %}
    {% do run_query(create_temp_table_query) %}

    select table_name, time_filter, actively_monitored, metrics
    from re_data_temporary_monitored_table
{% endmacro %}

{% macro tables_from_config_values(tables_def) %}
    {% for el in tables_def %}
        {% do format_metrics_from_config(el.metrics) %}
        (
                {{- full_table_name_values(el.table, el.schema, el.database) -}},
                {{- str_or_null(el.time_filter) -}},
                {{- bool_or_null(el.actively_monitored) -}},
                '{{ tojson(el.metrics) }}'
        ) {% if not loop.last %},{% endif %}
    {% endfor %}
{% endmacro %}

{% macro format_metrics_from_config(metrics) %}
    {{ adapter.dispatch('format_metrics_from_config', 're_data')(metrics) }}
{% endmacro %}

{% macro default__format_metrics_from_config(metrics) %}
    
{% endmacro %}

{% macro snowflake__format_metrics_from_config(metrics) %}
    {# /* Convert column names to upper case so it's detected by snowflake */ #}
    {% set column_metrics = {} %}
    {% for column in metrics.column %}
        {% set snowflake_column = column | upper %}
        {% do column_metrics.update({snowflake_column: metrics.column[column]}) %}
    {% endfor %}
    {% do metrics.update({'column': column_metrics}) %}
{% endmacro %}