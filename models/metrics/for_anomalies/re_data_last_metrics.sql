{% set metrics_tables =  ['re_data_base_metrics'] %}

{%- for table_name in metrics_tables %}

    select
        table_name,
        column_name,
        metric,
        value as last_value,
        interval_length_sec,
        computed_on
    from 
        {{ ref(table_name) }}
    where
        time_window_end = {{- time_window_end() -}}

    {%- if not loop.last %} union all {%- endif %}

{% endfor %}