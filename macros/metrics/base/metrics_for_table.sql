{% macro compute_metrics_for_tables(thread_value, ref_model) %}
    {%- set tables =  run_query(get_tables()) %}
    {%- for mtable in tables %}
        -- we are splitting computing metrics to 4 different threads
        {% set for_loop_mod = (loop.index % 4) %}
        {% if for_loop_mod == thread_value %}
            {% set table_name = row_value(mtable, 'table_name') %}
            {% set time_filter = row_value(mtable, 'time_filter') %}

            {% set columns_query %}
                select * from {{ ref('re_data_columns') }}
                where table_name = '{{ table_name }}'
            {% endset %}

            {% set columns = run_query(columns_query) %}

            {{ log('[re_data_log] - start computing metrics for table:' ~ table_name, True)}}

            {% set columns_to_query = [] %}
            {% set size = columns_to_query| length %}

            {% for column in columns %}
                {% do columns_to_query.append(column) %}
                {% set columns_size = columns_to_query| length %}

                {% if columns_size == 12 %}
                -- This seems to be size for which queries are small enough to be processed by DBs
                    {%- set insert_stats_query = get_insert_metrics_query(table_name, time_filter, ref_model, columns_to_query) -%}

                    {% if insert_stats_query %}
                        {% do run_query(insert_stats_query) %}
                    {% endif %}
                    {% do columns_to_query.clear() %}
                {% endif %}

            {% endfor %}

            {%- set insert_stats_query = get_insert_metrics_query(table_name, time_filter, ref_model, columns_to_query, table_level=True) -%}
            {% do run_query(insert_stats_query) %}

            {% set finish_timestamp = dbt_utils.current_timestamp() %} 
            {{ log('[re_data_log] - finished computing metrics for table:' ~ table_name, True)}}
        {% endif %}
    {% endfor %}
{% endmacro %}