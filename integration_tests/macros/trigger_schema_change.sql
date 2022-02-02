
{% macro schema_change_buy_events_add_column() %}
    {% set alter_table %}
        alter table {{ ref('buy_events')}} add column sample_column boolean
    {% endset %}
    {% do run_query(alter_table) %}
{% endmacro %}


{% macro schema_change_buy_events_drop_column() %}
    {% set alter_table %}
        alter table {{ ref('buy_events')}} drop column sample_column
    {% endset %}
    {% do run_query(alter_table) %}
{% endmacro %}