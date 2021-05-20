{% macro row_value(agate_row, column) %}
    {% set result = adapter.dispatch('row_value')(agate_row, column) %}
    {{ return(result) }}
{% endmacro %}

{% macro default__row_value(agate_row, column) %}
    {{ return (agate_row[column]) }}
{% endmacro %}


{% macro snowflake__row_value(agate_row, column) %}
    {{ return (agate_row[column.upper()])}}
{% endmacro %}