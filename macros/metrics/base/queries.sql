{% macro metrics_base_compute_for_thread(thread_value, ref_model) %}
    {%- set tables =  run_query(re_data.get_tables()) %}
    {%- for mtable in tables %}
        -- we are splitting computing metrics to 4 different threads
        {% set for_loop_mod = (loop.index % 4) %}
        {% if for_loop_mod == thread_value %}
            {% set name = re_data.row_value(mtable, 'name') %}
            {% set schema = re_data.row_value(mtable, 'schema') %}
            {% set database = re_data.row_value(mtable, 'database') %}
            {% set time_filter = re_data.row_value(mtable, 'time_filter') %}    
            {% set metrics = fromjson(re_data.row_value(mtable, 'metrics')) %}
            {% set for_cols = fromjson(re_data.row_value(mtable, 'columns')) %}
            {% set for_cols_dict = re_data.dict_from_list(for_cols) %}
            {% set table_name = re_data.full_table_name_values(name, schema, database) %}
 
            {% set columns_query %}
                select * from {{ ref('re_data_columns') }}
                where name = '{{ name }}' and schema = '{{ schema }}' and database = '{{ database }}'
            {% endset %}

            {% set columns = run_query(columns_query) %}

            {{ dbt_utils.log_info('[re_data_log] - start computing metrics for table:' ~ table_name) }}

            {% set columns_to_query = [] %}
            {% set size = columns_to_query| length %}

            {% for column in columns %}
                {% set column_name = re_data.row_value(column, 'column_name') %}
                

                {% if not for_cols_dict or (for_cols_dict.get(column_name)) %}
                    {% do columns_to_query.append(column) %}
                {% endif %}

                {% set columns_size = columns_to_query| length %}

                {% if columns_size == var('re_data:max_columns_in_query') %}
                {# /* Some balance size between making sure query will not crash &  */ #}
                    {%- set insert_stats_query = re_data.metrics_base_insert(table_name, time_filter, metrics, ref_model, columns_to_query) -%}

                    {% if insert_stats_query %}
                        {% do run_query(insert_stats_query) %}
                    {% endif %}
                    {% do columns_to_query.clear() %}
                {% endif %}
            {% endfor %}

            {%- set insert_stats_query = re_data.metrics_base_insert(table_name, time_filter, metrics, ref_model, columns_to_query, table_level=True) -%}
            {% do run_query(insert_stats_query) %}

            {{ dbt_utils.log_info('[re_data_log] - finished computing metrics for table:' ~ table_name) }}
        {% endif %}
    {% endfor %}
{% endmacro %}

{% macro metrics_base_insert(table_name, time_filter, metrics, ref_model, columns, table_level=False) %}

    {% set col_exprs = re_data.metrics_base_expressions(table_name, time_filter, metrics, columns, table_level) %}
    {% if col_exprs == [] %}
        {{ return ('') }}
    {% endif %}

    insert into {{ ref(ref_model) }}
    with temp_table_metrics as (
    select 
        {%- for col_expr in col_exprs %}
            ( {{ col_expr.expr }} ) as {{ col_expr.col_name + '___' + col_expr.metric }}
            {%- if not loop.last %},{%- endif %}
        {% endfor %}
    from 
        {{ table_name }}
    where
        {{ in_time_window(time_filter) }}
    )

    {%- for col_expr in col_exprs %}
        {% set final_metric_name = get_final_metric_name(col_expr.metric, time_filter) %}
        
        select '{{table_name}}' as table_name, '{{ col_expr.col_name }}' as column_name, '{{ final_metric_name }}' as metric, {{ col_expr.col_name + '___' + col_expr.metric }} as value
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