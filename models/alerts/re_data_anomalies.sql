{{
    config(
        materialized='view'
    )
}}

{% set sql %}
select
name,schema,database,{{json_extract('anomaly_detector','filter')}} filter
from {{ ref('re_data_selected') }}
where {{ json_extract('anomaly_detector', 'filter') }} is not null
{% endset %}

{%- set filters = dbt_utils.get_query_results_as_dict(sql) -%}

select
    z.id,
    z.table_name,
    z.column_name,
    z.metric,
    z.z_score_value,
    z.modified_z_score_value,
    m.anomaly_detector,
    z.last_value,
    z.last_avg,
    z.last_median,
    z.last_stddev,
    z.last_median_absolute_deviation,
    z.last_mean_absolute_deviation,
    z.last_iqr,
    z.last_first_quartile - (cast( {{ json_extract('m.anomaly_detector', 'whisker_boundary_multiplier') }} as {{numeric_type()}} ) * z.last_iqr) lower_bound,
    z.last_third_quartile + (cast( {{ json_extract('m.anomaly_detector', 'whisker_boundary_multiplier') }} as {{numeric_type()}} ) * z.last_iqr) upper_bound,
    z.last_first_quartile,
    z.last_third_quartile,
    z.time_window_end,
    z.interval_length_sec,
    z.computed_on,
    {{ re_data.generate_anomaly_message('z.column_name', 'z.metric', 'z.last_value', 'z.last_avg') }} as message,
    {{ re_data.generate_metric_value_text('z.metric', 'z.last_value') }} as last_value_text
from
    {{ ref('re_data_z_score')}} z
left join {{ ref('re_data_selected') }} m
on {{ split_and_return_nth_value('table_name', '.', 1) }} = m.database
and {{ split_and_return_nth_value('table_name', '.', 2) }} = m.schema
and {{ split_and_return_nth_value('table_name', '.', 3) }} = m.name
where
    case
    {% for filter in filters["filter"] %}
      when m.database='{{ filters["database"][loop.index0] }}' and m.schema='{{ filters["schema"][loop.index0] }}' and m.name='{{ filters["name"][loop.index0] }}'
      then {{ filter }}
    {% endfor %}
    else true
    end
    and
    case
        when {{ json_extract('m.anomaly_detector', 'name') }} = 'z_score'
            then abs(z_score_value) > cast({{ json_extract('m.anomaly_detector', 'threshold') }} as {{ numeric_type() }})
        when {{ json_extract('m.anomaly_detector', 'name') }} = 'modified_z_score'
            then abs(modified_z_score_value) > cast( {{ json_extract('m.anomaly_detector', 'threshold') }} as {{numeric_type()}} )
        when {{ json_extract('m.anomaly_detector', 'name') }} = 'boxplot'
            then (
                z.last_value < z.last_first_quartile - (cast( {{ json_extract('m.anomaly_detector', 'whisker_boundary_multiplier') }} as {{numeric_type()}} ) * z.last_iqr)
                or
                z.last_value > z.last_third_quartile + (cast( {{ json_extract('m.anomaly_detector', 'whisker_boundary_multiplier') }} as {{numeric_type()}} ) * z.last_iqr)
            )
        else false
    end
