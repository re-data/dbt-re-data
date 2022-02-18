{% set metrics_tables = ['re_data_base_metrics'] %}

{%- for table_name in metrics_tables %}
    with median_value as (
        select 
            table_name,
            column_name,
            metric,
            interval_length_sec,
            {{ fivetran_utils.percentile(percentile_field='value', partition_field='table_name, column_name, metric, interval_length_sec', percent='0.5') }} as last_median
        from
            {{ ref(table_name) }}
        where
            time_window_end > {{- anamaly_detection_time_window_start() -}} and
            time_window_end <= {{- time_window_end() -}}
        {% if target.type == 'postgres' %} 
            group by
                table_name, column_name, metric, interval_length_sec
        {% endif %}
        
    ), abs_deviation as (
        select 
            table_name,
            column_name,
            metric,
            interval_length_sec,
            abs(
                    value
                        - 
                    avg(value) over(partition by table_name, column_name, metric, interval_length_sec)
                ) as absolute_deviation
        from
            {{ ref(table_name) }}
        where
            time_window_end > {{- anamaly_detection_time_window_start() -}} and
            time_window_end <= {{- time_window_end() -}}
    ), median_abs_deviation as (
        select
            table_name,
            column_name,
            metric,
            interval_length_sec,
            {{ fivetran_utils.percentile(percentile_field='absolute_deviation', partition_field='table_name, column_name, metric, interval_length_sec', percent='0.5') }} as median_absolute_deviation
        from
            abs_deviation
        {% if target.type == 'postgres' %} 
            group by
                table_name, column_name, metric, interval_length_sec
        {% endif %}
    ), stats as (
        select
            table_name,
            column_name,
            metric,
            avg(value) as last_avg,
            stddev(value) as last_stddev,
            max(time_window_end) as last_metric_time,
            interval_length_sec,
            max(computed_on) as computed_on
        from
            {{ ref(table_name) }}
        where
            time_window_end > {{- anamaly_detection_time_window_start() -}} and
            time_window_end <= {{- time_window_end() -}}
        group by
            table_name, column_name, metric, interval_length_sec
    )
    select distinct
        s.table_name,
        s.column_name,
        s.metric,
        s.last_avg,
        s.last_stddev,
        s.last_metric_time,
        s.interval_length_sec,
        s.computed_on,
        mv.last_median,
        md.median_absolute_deviation last_median_absolute_deviation
    from
        stats s
    left join
        median_value mv
        on
            s.table_name = mv.table_name and
            s.column_name = mv.column_name and
            s.metric = mv.metric and
            s.interval_length_sec = mv.interval_length_sec
    left join
        median_abs_deviation md
        on 
            s.table_name = md.table_name and
            s.column_name = md.column_name and
            s.metric = md.metric and
            s.interval_length_sec = md.interval_length_sec

    {%- if not loop.last %} union all {%- endif %}    

{% endfor %}