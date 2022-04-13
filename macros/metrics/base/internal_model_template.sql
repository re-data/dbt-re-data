{% macro re_data_last_base_metrics_part() %}

-- depends_on: {{ ref('re_data_columns') }}

{{
    config(
        materialized='table',
    )
}}

{{ re_data.empty_last_base_metrics() }}

{% endmacro %}

{% macro re_data_last_base_metrics_thread(num) %}
    {% set part_name = 're_data_last_base_metrics_part' ~ num %}
    {{ re_data.generate_depends(['re_data_monitored', 're_data_columns', 're_data_run_started_at', part_name]) }}

    {{
        config(
            materialized='table',
        )
    }}

    {% if not re_data.in_compile() %}
        {{ re_data.metrics_base_compute_for_thread(num, part_name) }}
    {% endif %}

    {{ re_data.empty_last_base_metrics() }}

{% endmacro %}