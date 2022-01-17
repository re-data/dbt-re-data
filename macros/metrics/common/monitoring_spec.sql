
{% macro get_monitoring_spec(par_name, table, group) %}
    {% if table.get(par_name) is not none %}
        {{ return (table[par_name]) }}
    {% elif group.get(par_name) is not none %}
        {{ return (group[par_name]) }}
    {% else %}
        {{ return (None) }}
    {% endif %}
{% endmacro %}

{% macro get_schema_spec(par_name, table, group) %}
    {% set name = table['name'] %}
    {% set package = project_name %}

    {% if table[par_name] %}
        {{ return (table[par_name]) }}
    {% elif group[par_name] %}
        {{ return (group[par_name]) }}
    {% else %}
        {% set suffix = '.' + package + '.' + name %}
        {% set check_source_name = 'source.' + package + '.'%}
        {% set graph_node = graph.nodes.get('model' + suffix) or graph.nodes.get('seed' + suffix) or graph.sources.get('source' + '.' + opt_schema + suffix) %}
        {% if graph_node %}
            {{ return (graph_node[par_name]) }}
        {% else %}
            {{ debug()}}
            {{ log('[re_data_log] - error no dbt graph node found for ' ~ table, True) }}
            {{ return (none) }}
        {% endif %}
    {% endif %}
{% endmacro %}

{% macro monitoring_spec(table, group) %}
    
    {% set name = table['name'] %}
    {% set schema = get_schema_spec('schema', table, group) %}
    {% set database = get_schema_spec('database', table, group) %}
    {% set actively_monitored = get_monitoring_spec('actively_monitored', table, group) %}
    {% set time_filter = get_monitoring_spec('time_filter', table, group) %}
    {% set metrics = table.get('metrics', {}) %}
    {% set columns = table.get('columns', []) %}
    
    {{ return ([{'table': name, 'schema': schema, 'database': database, 'time_filter': time_filter, 'actively_monitored': actively_monitored, 'metrics': metrics, 'columns': columns}]) }}

{% endmacro %}