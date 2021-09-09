
{% macro get_monitoring_spec(par_name, table, group) %}
    {% if table.get(par_name) is not none %}
        {{ return (table[par_name]) }}
    {% elif group.get(par_name) is not none %}
        {{ return (group[par_name]) }}
    {% else %}
        {{ return (None) }}
    {% endif %}
{% endmacro %}

{% macro obj_graph_name(obj_type, name) %}
    {{ return (obj_type + '.' + project_name + '.' + name) }}
{% endmacro %}

{% macro get_schema_spec(par_name, table, group) %}
    {% set name = table['name'] %}

    {% if table[par_name] %}
        {{ return (table[par_name]) }}
    {% elif group[par_name] %}
        {{ return (group[par_name]) }}
    {% elif graph.nodes.get(obj_graph_name('model', name)) %}
        {{ return (graph.nodes.get(obj_graph_name('model', name))[par_name]) }}

    {% elif graph.nodes.get(obj_graph_name('seed', name)) %}
        {{ return (graph.nodes.get(obj_graph_name('seed', name))[par_name]) }}
    
    {% elif graph.sources.get(obj_graph_name('source', name)) %}
        {{ return (graph.nodes.get(obj_graph_name('source', name))[par_name]) }}
    
    {% else %}
        {{ retrun (None) }}
    {% endif %}
{% endmacro %}

{% macro monitoring_spec(table, group) %}

    {% set name = table['name'] %}
    {% set schema = get_schema_spec('schema', table, group) %}
    {% set database = get_schema_spec('database', table, group) %}
    {% set actively_monitored = get_monitoring_spec('actively_monitored', table, group) %}
    {% set time_filter = get_monitoring_spec('time_filter', table, group) %}
    {% set metrics = table.get('metrics', {}) %}
    

    {{ return ([{'table': name, 'schema': schema, 'database': database, 'time_filter': time_filter, 'actively_monitored': actively_monitored, 'metrics': metrics}]) }}

{% endmacro %}
