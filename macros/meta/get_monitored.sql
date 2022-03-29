{% macro pub_monitored_from_graph() %}
    {% set monitored = [] %}
    {% set both = []%}
    {% do both.extend(graph.nodes.values()) %}
    {% do both.extend(graph.sources.values()) %}
    {% set slack_config = re_data.get_slack_config() %}

    {% for el in both %}
        {% if el.resource_type in ['model', 'seed', 'source'] %}
            {% if el.config.get('re_data_monitored') %}
            
                {% do monitored.append({
                    'name': re_data.name_in_db(el.identifier or el.name),
                    'schema': re_data.name_in_db(el.schema),
                    'database': re_data.name_in_db(el.database),
                    'time_filter': el.config.get('re_data_time_filter', none),
                    'metrics': re_data.metrics_in_db(el.config.get('re_data_metrics', {})),
                    'columns': re_data.columns_in_db(el.config.get('re_data_columns', [])),
                    'anomaly_detector': el.config.get('re_data_anomaly_detector', var('re_data:anomaly_detector', {})),
                    'slack_owners': re_data.prepare_slack_model_owners(el.config.get('re_data_slack_owners', []), slack_config),
                    })
                %}
            {% endif %}
        {% endif %}
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
    
    {{ return ([{
        'name': re_data.name_in_db(name),
        'schema': re_data.name_in_db(schema),
        'database': re_data.name_in_db(database),
        'time_filter': time_filter,
        'metrics': re_data.metrics_in_db(metrics),
        'columns': re_data.columns_in_db(columns),
        'anomaly_detector': var('re_data:anomaly_detector', {}),
        'slack_owners': {}
        }]) 
    }}

{% endmacro %}

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

{% macro get_slack_config() %}
    {% set slack_config = var('re_data:slack_config', {}) %}
    {{ return (slack_config) }}
{% endmacro %}

{% macro prepare_slack_model_owners(slack_owners, slack_config) %}
    {# 
        loop through slack_owners that have member set in re_data:slack_config.
        if the owner is defined as a group in re_data:slack_config, then add the group members to the slack_owners list.
        e.g.
        user1: xxx
        user2: yyy
        user3: zzz
        team: [user1, user2]

        if team is passed in as a slack_owner, then the slack_owners list will contain user1, user2.
        This is used to mention the owners in the slack message individually.
     #}
    {% set owners = {} %}
    {% for owner in slack_owners if slack_config.get(owner) %}
        {% set member = slack_config.get(owner) %}
        {% if re_data.is_list(member) %}
            {% for key in member %}
                {% if key in slack_config %}
                    {% do owners.update({key: slack_config[key] }) %}
                {% endif %}
            {% endfor %}
        {% else %}
            {% do owners.update({owner: member }) %}
        {% endif %}
    {% endfor %}

    {{ return (owners) }}
{% endmacro %}