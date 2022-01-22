{% macro pub_monitored_from_graph %}
    {% set monitored = [] %}
    {% for node in graph.nodes %}
    {% endfor %}

    {{ return(monitored) }}
{% endmacro %}

{% macro pub_monitored_from_vars() %}
    {% set monitored = [] %}
    {% for group in var('re_data:monitored') %}
        {% for table in group['tables'] %}
            {% do monitored.extend(priv_get_monitored_from_group(table, group)) %}
        {% endfor %}
    {% endfor %}

    {{ return(monitored) }}
{% endmacro %}

{% macro priv_get_monitored_from_group(table, group) %}
    
    {% set name = table['name'] %}
    {% set schema = priv_schema_spec('schema', table, group) %}
    {% set database = priv_schema_spec('database', table, group) %}
    {% set time_filter = priv_monitored_spec('time_filter', table, group) %}
    {% set metrics = table.get('metrics', {}) %}
    {% set columns = table.get('columns', []) %}
    
    {{ return ([{'name': name, 'schema': schema, 'database': database, 'time_filter': time_filter, 'metrics': metrics, 'columns': columns}]) }}

{% macro priv_monitored_spec(par_name, table, group) %}
    {% if table.get(par_name) is not none %}
        {{ return (table[par_name]) }}
    {% elif group.get(par_name) is not none %}
        {{ return (group[par_name]) }}
    {% else %}
        {{ return (none) }}
    {% endif %}
{% endmacro %}

{% macro priv_schema_spec(par_name, table, group) %}
    {% set name = table['name'] %}
    {% set package = project_name %}

    {% if table[par_name] %}
        {{ return (table[par_name]) }}
    {% elif group[par_name] %}
        {{ return (group[par_name]) }}
    {% else %}
        {% set suffix = '.' + package + '.' + name %}
        {% set ns = namespace(graph_node=graph.nodes.get('model' + suffix) or graph.nodes.get('seed' + suffix)) %}    
        {% if not ns.graph_node %}
            {% set schema_name = table.get('schema') or target.schema %}
            {% set source_name = 'source' + '.' + project_name + '.' + schema_name + '.' + table['name'] %}
            {% set ns.graph_node = graph.sources.get(source_name) %}
        {% endif %}

        {% if ns.graph_node %}
            {{ return (ns.graph_node[par_name]) }}
        {% else %}
            {{ log('[re_data_log] - error no dbt graph node found for ' ~ table, True) }}
            {{ return (none) }}
        {% endif %}
    {% endif %}
{% endmacro %}

{% endmacro %}