{% macro re_data_last_base_metrics_part() %}

-- depends_on: {{ ref('re_data_columns') }}

{{
    config(
        materialized='table',
    )
}}

{{ empty_last_base_metrics() }}

{% endmacro %}

{% macro re_data_last_base_metrics_thread(num) %}
    {% set part_name = 're_data_last_base_metrics_part' ~ num %}
    {{ generate_depends(['re_data_columns', 're_data_run_started_at', 're_data_tables', part_name]) }}

    {{
        config(
            materialized='table',
        )
    }}

    {% if not in_compile() %}
        {{ compute_metrics_for_tables(num, part_name) }}
    {% endif %}

    {{ empty_last_base_metrics() }}

{% endmacro %}