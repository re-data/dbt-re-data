{{ config(materialized="view") }}
select
    z.id,
    z.table_name,
    z.column_name,
    z.metric,
    z.z_score_value,
    z.modified_z_score_value,
    m.anomaly_detector,
    c.metric_spec,
    z.last_value,
    z.last_avg,
    z.last_median,
    z.last_stddev,
    z.last_median_absolute_deviation,
    z.last_mean_absolute_deviation,
    z.last_iqr,
    z.last_first_quartile - (
        cast(
            {{ json_extract("m.anomaly_detector", "whisker_boundary_multiplier") }}
            as {{ numeric_type() }}
        )
        * z.last_iqr
    ) lower_bound,
    z.last_third_quartile + (
        cast(
            {{ json_extract("m.anomaly_detector", "whisker_boundary_multiplier") }}
            as {{ numeric_type() }}
        )
        * z.last_iqr
    ) upper_bound,
    z.last_first_quartile,
    z.last_third_quartile,
    z.time_window_end,
    z.interval_length_sec,
    z.computed_on,
    {{
        re_data.generate_anomaly_message(
            "z.column_name", "z.metric", "z.last_value", "z.last_avg"
        )
    }} as message,
    {{ re_data.generate_metric_value_text("z.metric", "z.last_value") }}
    as last_value_text
from {{ ref("re_data_z_score") }} z
left join
    {{ ref("re_data_selected") }} m
    on {{ split_and_return_nth_value("table_name", ".", 1) }} = m.database
    and {{ split_and_return_nth_value("table_name", ".", 2) }} = m.schema
    and {{ split_and_return_nth_value("table_name", ".", 3) }} = m.name
left join
    {{ ref("re_data_selected_columns") }} c
    on {{ split_and_return_nth_value("table_name", ".", 1) }} = c.database
    and {{ split_and_return_nth_value("table_name", ".", 2) }} = c.schema
    and {{ split_and_return_nth_value("table_name", ".", 3) }} = c.name
    and z.column_name = c.column
where
    case
        when c.metric_spec is not null
        then
            {{
                is_anomaly_from_model(
                    anomaly_config="c.metric_spec",
                    last_value="z.last_value",
                    last_avg="z.last_avg",
                    z_score_value="z.z_score_value",
                    modified_z_score_value="z.modified_z_score_value",
                    last_first_quartile="z.last_first_quartile",
                    last_iqr="z.last_iqr",
                    last_third_quartile="z.last_third_quartile",
                )
            }}
        else
            {{
                is_anomaly_from_model(
                    anomaly_config="m.anomaly_detector",
                    last_value="z.last_value",
                    last_avg="z.last_avg",
                    z_score_value="z.z_score_value",
                    modified_z_score_value="z.modified_z_score_value",
                    last_first_quartile="z.last_first_quartile",
                    last_iqr="z.last_iqr",
                    last_third_quartile="z.last_third_quartile",
                )
            }}

    end
