
{% macro get_target_database() %}
    {{- adapter.dispatch('get_target_database', 're_data')() -}}
{% endmacro %}

{% macro default__get_target_database() %}
    {{- return (target.dbname) -}}
{% endmacro %}

{% macro bigquery__get_target_database() %}
    {{- return (target.project) -}}
{% endmacro %}

{% macro snowflake__get_target_database() %}
    {{- return (target.database) -}}
{% endmacro %}