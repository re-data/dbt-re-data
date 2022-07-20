{% macro metrics_base_compute_for_thread(thread_value, ref_model) %}
    {%- set tables =  run_query(re_data.get_tables()) %}
    {%- for mtable in tables %}
        -- we are splitting computing metrics to 4 different threads
        {% set for_loop_mod = (loop.index % 4) %}
        {% if for_loop_mod == thread_value %}
            {% set model = get_model_config(mtable) %}

            {% set columns_to_query = [] %}
            {% set size = 0 %}

            {% for column in model.columns %}
                {% set column_name = re_data.row_value(column, 'column_name') %}
                
                {% if should_compute_metric(model, column_name) %}
                    {% do columns_to_query.append(column) %}
                {% endif %}

                {% set columns_size = columns_to_query| length %}

                {% if columns_size == var('re_data:max_columns_in_query') %}
                    {%- set insert_stats_query = re_data.metrics_base_insert(model, ref_model, columns_to_query) -%}

                    {% if insert_stats_query %}
                        {% do run_query(insert_stats_query) %}
                    {% endif %}
                    {% do columns_to_query.clear() %}
                {% endif %}
            {% endfor %}

            {%- set insert_stats_query = re_data.metrics_base_insert(model, ref_model, columns_to_query, table_level=True) -%}
            {% do run_query(insert_stats_query) %}

            {{ dbt_utils.log_info('[re_data_log] - finished computing metrics for:' ~ model.model_name) }}
        {% endif %}
    {% endfor %}
{% endmacro %}

{% macro metrics_base_insert(model, ref_model, columns, table_level=False) %}

    {% set col_exprs = re_data.metrics_base_expressions(model, columns, table_level) %}
    {% if col_exprs == [] %}
        {{ return ('') }}
    {% endif %}

    insert into {{ ref(ref_model) }}
    with temp_table_metrics as (
    select 
        {%- for col_expr in col_exprs %}
            ( {{ col_expr.expr }} ) as {{ re_data.quote_column_name(col_expr.col_name + '___' + col_expr.metric) }}
            {%- if not loop.last %},{%- endif %}
        {% endfor %}
    from 
        {{ model.table_name }}
    where
        {{ in_time_window(model.time_filter) }}
    )

    {%- for col_expr in col_exprs %}
        {% set final_metric_name = get_final_metric_name(col_expr.metric, model.time_filter) %}
        
        select '{{model.table_name}}' as table_name, '{{ col_expr.col_name }}' as column_name, '{{ final_metric_name }}' as metric, {{ re_data.quote_column_name(col_expr.col_name + '___' + col_expr.metric) }} as value
        from temp_table_metrics
        {% if not loop.last %}union all{% endif %}
    {% endfor %}

{% endmacro %}

{% macro get_final_metric_name(metric_name, time_filter) %}
    {% if time_filter is none %}
        {{ return ('global__' + metric_name) }}
    {% else %}
        {{ return (metric_name) }}
    {% endif %}
{% endmacro %}