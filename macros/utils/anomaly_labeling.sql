{% macro change_percentage(last_value, last_avg) %}
    cast({{ last_value }} - {{ last_avg }} as float64)
    / nullif(cast({{ last_avg }} as float64), 0)
    * 100.0
{% endmacro %}

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


{% macro is_anomaly_absolute_threshold(anomaly_config, last_value) %}
    case
        when
            {{
                json_extract(
                    anomaly_config,
                    "absolute_threshold.threshold",
                )
            }} is not null
        then
            case
                when

                    lower(
                        {{
                            json_extract(
                                anomaly_config,
                                "absolute_threshold.direction",
                            )
                        }}
                    )
                    = 'up'
                then
                    {{ last_value }} > cast(
                        {{
                            json_extract(
                                anomaly_config,
                                "absolute_threshold.threshold",
                            )
                        }} as {{ numeric_type() }}
                    )

                when
                    lower(
                        {{
                            json_extract(
                                anomaly_config,
                                "absolute_threshold.direction",
                            )
                        }}
                    )
                    = 'down'
                then
                    {{ last_value }} < cast(
                        {{
                            json_extract(
                                anomaly_config,
                                "absolute_threshold.threshold",
                            )
                        }} as {{ numeric_type() }}
                    )
                when
                    lower(
                        coalesce(
                            {{
                                json_extract(
                                    anomaly_config,
                                    "absolute_threshold.direction",
                                )
                            }},
                            'both'
                        )
                    )
                    = 'both'
                then
                    abs({{ last_value }}) > cast(
                        {{
                            json_extract(
                                anomaly_config,
                                "absolute_threshold.threshold",
                            )
                        }} as {{ numeric_type() }}
                    )
            end
        else true
    end

{% endmacro %}

{% macro is_anomaly_change_percentage(anomaly_config, last_value, last_avg) %}
    case
        when
            {{
                json_extract(
                    anomaly_config,
                    "change_percentage.threshold",
                )
            }} is not null
        then
            case
                when

                    lower(
                        {{
                            json_extract(
                                anomaly_config,
                                "change_percentage.direction",
                            )
                        }}
                    )
                    = 'up'
                then
                    (
                        {{
                            change_percentage(
                                last_value=last_value, last_avg=last_avg
                            )
                        }}
                    ) > cast(
                        {{
                            json_extract(
                                anomaly_config,
                                "change_percentage.threshold",
                            )
                        }} as {{ numeric_type() }}
                    )

                when
                    lower(
                        {{
                            json_extract(
                                anomaly_config,
                                "change_percentage.direction",
                            )
                        }}
                    )
                    = 'down'
                then
                    (
                        {{
                            change_percentage(
                                last_value=last_value, last_avg=last_avg
                            )
                        }}
                    ) < (
                        0.0 - (
                            cast(
                                {{
                                    json_extract(
                                        anomaly_config,
                                        "change_percentage.threshold",
                                    )
                                }} as {{ numeric_type() }}
                            )
                        )
                    )
                when
                    lower(
                        coalesce(
                            {{
                                json_extract(
                                    anomaly_config,
                                    "change_percentage.direction",
                                )
                            }},
                            'both'
                        )
                    )
                    = 'both'
                then
                    abs(
                        {{
                            change_percentage(
                                last_value=last_value, last_avg=last_avg
                            )
                        }}
                    ) > cast(
                        {{
                            json_extract(
                                anomaly_config,
                                "change_percentage.threshold",
                            )
                        }} as {{ numeric_type() }}
                    )
            end
        else true
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
