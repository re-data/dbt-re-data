
{% macro get_monitoring_spec(par_name, table, group) %}
    {% if table.get(par_name) is not none %}
        {{ return (table[par_name]) }}
    {% elif group.get(par_name) is not none %}
        {{ return (group[par_name]) }}
    {% else %}
        {{ return (None) }}
    {% endif %}
{% endmacro %}

{% macro graph_param(param, name, package, check_types) %}
    {% for t in check_types %}
        {% if graph.nodes.get(t + '.' + package + '.' + name)[param] %}
            {{ return (graph.nodes[t + '.' + package + '.' + name][param]) }}
        {% endif %}
    {% endfor %}

    {{ return (none) }}

{% endmacro %}

{% macro get_schema_spec(par_name, table, group) %}
    {% set name = table['name'] %}
    {% set package = project_name %}

    {% if table[par_name] %}
        {{ return (table[par_name]) }}
    {% elif group[par_name] %}
        {{ return (group[par_name]) }}
    {% else %}
        {% set value = graph_param(par_name, name, package, ['source', 'seed', 'model']) %}
        {{ return (value) }}
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
