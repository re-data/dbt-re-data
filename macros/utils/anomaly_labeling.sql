{% macro is_anomaly_from_model(
    anomaly_config,
    last_value,
    last_avg,
    z_score_value,
    modified_z_score_value,
    last_first_quartile,
    last_iqr,
    last_third_quartile
) %}
    case
        when
            (
                lower(coalesce({{ json_extract(anomaly_config, "direction") }}, 'both'))
                = 'up'
                and {{ last_value }} > {{ last_avg }}
            )
            or (
                lower(coalesce({{ json_extract(anomaly_config, "direction") }}, 'both'))
                = 'down'
                and {{ last_value }} < {{ last_avg }}
            )
            or (
                lower(coalesce({{ json_extract(anomaly_config, "direction") }}, 'both'))
                != 'up'
                and lower(
                    coalesce({{ json_extract(anomaly_config, "direction") }}, 'both')
                )
                != 'down'
            )
        then
            case
                when {{ json_extract(anomaly_config, "name") }} = 'z_score'
                then
                    abs({{ z_score_value }}) > cast(
                        {{ json_extract(anomaly_config, "threshold") }}
                        as {{ numeric_type() }}
                    )
                when {{ json_extract(anomaly_config, "name") }} = 'modified_z_score'
                then
                    abs({{ modified_z_score_value }}) > cast(
                        {{ json_extract(anomaly_config, "threshold") }}
                        as {{ numeric_type() }}
                    )
                when {{ json_extract(anomaly_config, "name") }} = 'boxplot'
                then
                    (
                        {{ last_value }}
                        < {{ last_first_quartile }}
                        - (
                            cast(
                                {{
                                    json_extract(
                                        anomaly_config,
                                        "whisker_boundary_multiplier",
                                    )
                                }} as {{ numeric_type() }}
                            )
                            * {{ last_iqr }}
                        )
                        or {{ last_value }}
                        > {{ last_third_quartile }}
                        + (
                            cast(
                                {{
                                    json_extract(
                                        anomaly_config,
                                        "whisker_boundary_multiplier",
                                    )
                                }} as {{ numeric_type() }}
                            )
                            * {{ last_iqr }}
                        )
                    )
                else false
            end
        else false
    end
{% endmacro %}


{% macro is_anomaly_from_column(
    anomaly_config,
    last_value,
    last_avg,
    z_score_value,
    modified_z_score_value,
    last_first_quartile,
    last_iqr,
    last_third_quartile
) %}
    case
        when
            (
                lower(
                    coalesce(
                        {{
                            json_extract(
                                anomaly_config,
                                "re_data_anomaly_detector.direction",
                            )
                        }},
                        'both'
                    )
                )
                = 'up'
                and {{ last_value }} > {{ last_avg }}
            )
            or (
                lower(
                    coalesce(
                        {{
                            json_extract(
                                anomaly_config,
                                "re_data_anomaly_detector.direction",
                            )
                        }},
                        'both'
                    )
                )
                = 'down'
                and {{ last_value }} < {{ last_avg }}
            )
            or (
                lower(
                    coalesce(
                        {{
                            json_extract(
                                anomaly_config,
                                "re_data_anomaly_detector.direction",
                            )
                        }},
                        'both'
                    )
                )
                != 'up'
                and lower(
                    coalesce(
                        {{
                            json_extract(
                                anomaly_config,
                                "re_data_anomaly_detector.direction",
                            )
                        }},
                        'both'
                    )
                )
                != 'down'
            )
        then
            case
                when
                    {{ json_extract(anomaly_config, "re_data_anomaly_detector.name") }}
                    = 'z_score'
                then
                    abs({{ z_score_value }}) > cast(
                        {{
                            json_extract(
                                anomaly_config,
                                "re_data_anomaly_detector.threshold",
                            )
                        }} as {{ numeric_type() }}
                    )
                when
                    {{ json_extract(anomaly_config, "re_data_anomaly_detector.name") }}
                    = 'modified_z_score'
                then
                    abs({{ modified_z_score_value }}) > cast(
                        {{
                            json_extract(
                                anomaly_config,
                                "re_data_anomaly_detector.threshold",
                            )
                        }} as {{ numeric_type() }}
                    )
                when
                    {{ json_extract(anomaly_config, "re_data_anomaly_detector.name") }}
                    = 'boxplot'
                then
                    (
                        {{ last_value }}
                        < {{ last_first_quartile }}
                        - (
                            cast(
                                {{
                                    json_extract(
                                        anomaly_config,
                                        "re_data_anomaly_detector.whisker_boundary_multiplier",
                                    )
                                }}
                                as {{ numeric_type() }}
                            )
                            * {{ last_iqr }}
                        )
                        or {{ last_value }}
                        > {{ last_third_quartile }}
                        + (
                            cast(
                                {{
                                    json_extract(
                                        anomaly_config,
                                        "re_data_anomaly_detector.whisker_boundary_multiplier",
                                    )
                                }}
                                as {{ numeric_type() }}
                            )
                            * {{ last_iqr }}
                        )
                    )
                else false
            end
        else false
    end
{% endmacro %}
