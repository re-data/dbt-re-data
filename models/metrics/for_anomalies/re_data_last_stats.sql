{% set metrics_tables = ['re_data_base_metrics'] %}
{% set columns_to_group_by = 'table_name, column_name, metric, interval_length_sec' %}

{%- for table_name in metrics_tables %}

    with median_value as (
        select distinct
            table_name,
            column_name,
            metric,
            interval_length_sec,
            avg(value) {% if target.type != 'postgres' %} over(partition by {{ columns_to_group_by }}) {% endif %} as last_avg,
            {{ fivetran_utils.percentile(percentile_field='value', partition_field=columns_to_group_by, percent='0.25') }} as last_first_quartile,
            {{ fivetran_utils.percentile(percentile_field='value', partition_field=columns_to_group_by, percent='0.5') }} as last_median,
            {{ fivetran_utils.percentile(percentile_field='value', partition_field=columns_to_group_by, percent='0.75') }} as last_third_quartile
        from
            {{ ref(table_name) }}
        where
            time_window_end > {{- anamaly_detection_time_window_start() -}} and
            time_window_end <= {{- time_window_end() -}}
        {% if target.type == 'postgres' %} 
            group by
                {{ columns_to_group_by }}
        {% endif %}
        
    ), abs_deviation as (
        select 
            s.table_name,
            s.column_name,
            s.metric,
            s.interval_length_sec,
            abs( s.value - mv.last_avg ) as absolute_deviation_from_mean,
            abs( s.value - mv.last_median ) as absolute_deviation_from_median
        from
            {{ ref(table_name) }} s
        left join 
            median_value mv
            on
                s.table_name = mv.table_name and
                s.column_name = mv.column_name and
                s.metric = mv.metric and
                s.interval_length_sec = mv.interval_length_sec
        where
            s.time_window_end > {{- anamaly_detection_time_window_start() -}} and
            s.time_window_end <= {{- time_window_end() -}}
    ), median_abs_deviation as (
        select distinct
            table_name,
            column_name,
            metric,
            interval_length_sec,
            avg(absolute_deviation_from_mean) {% if target.type != 'postgres' %} over(partition by {{ columns_to_group_by }}) {% endif %} as mean_absolute_deviation,
            {{ fivetran_utils.percentile(percentile_field='absolute_deviation_from_median', partition_field=columns_to_group_by, percent='0.5') }} as median_absolute_deviation
        from
            abs_deviation
        {% if target.type == 'postgres' %} 
            group by
                {{ columns_to_group_by }}
        {% endif %}
    ), stats as (
        select
            table_name,
            column_name,
            metric,
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
            {{ columns_to_group_by }}
    )
    select
        s.table_name,
        s.column_name,
        s.metric,
        mv.last_avg,
        s.last_stddev,
        s.last_metric_time,
        s.interval_length_sec,
        s.computed_on,
        mv.last_median,
        mv.last_first_quartile,
        mv.last_third_quartile,
        md.median_absolute_deviation last_median_absolute_deviation,
        md.mean_absolute_deviation last_mean_absolute_deviation
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