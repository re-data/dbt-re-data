
{% macro store_results() %}
    -- depends_on: {{ ref('re_data_run_results') }}
    {% set results = var('results') %}
    {% set run_dict = {"results": results} %}

    {% if execute and results %}

        {% set insert_query %}
            insert into {{ ref('re_data_run_results') }}
            values (
                {{ quote_text(run_dict) }}, {{dbt_utils.current_timestamp_in_utc()}}
            )
        {% endset %}

        {% do run_query(insert_query) %}
    {% endif %}

{% endmacro %}