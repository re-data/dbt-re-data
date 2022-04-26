{% macro pub_monitored_from_graph() %}
    {% set monitored = [] %}
    {% set both = []%}
    {% do both.extend(graph.nodes.values()) %}
    {% do both.extend(graph.sources.values()) %}
    {% set owners_config = re_data.get_owners_config() %}

    {% for el in both %}
        {% if el.resource_type in ['model', 'seed', 'source'] %}
            {% if el.config.get('re_data_monitored') %}
                {% do monitored.append({
                    'name': re_data.name_in_db(el.identifier or el.alias or el.name),
                    'schema': re_data.name_in_db(el.schema),
                    'database': re_data.name_in_db(el.database),
                    'time_filter': el.config.get('re_data_time_filter', none),
                    'metrics': re_data.metrics_in_db(el.config.get('re_data_metrics', {})),
                    'columns': re_data.columns_in_db(el.config.get('re_data_columns', [])),
                    'anomaly_detector': el.config.get('re_data_anomaly_detector', var('re_data:anomaly_detector', {})),
                    'owners': re_data.prepare_model_owners(el.config.get('re_data_owners', []), owners_config),
                    })
                %}
            {% endif %}
        {% endif %}
    {% endfor %}

    {{ return(monitored) }}
{% endmacro %}

{% macro get_owners_config() %}
    {% set owners_config = var('re_data:owners_config', {}) %}
    {{ return (owners_config) }}
{% endmacro %}

{% macro prepare_model_owners(re_data_owners, owners_config) %}
    {% set owners = {} %}
    {% set seen_identifiers = {} %}
    {% for owner in re_data_owners if owners_config.get(owner) %}
        {% set members = owners_config.get(owner) %}
        {% for member in members %}
            {% set identifier = member.get('identifier') %}
            {% if identifier not in seen_identifiers %}
            {% do seen_identifiers.update({identifier: true }) %}
            {% do owners.update({
                identifier: {
                    'notify_channel': member.get('type'),
                    'owner': owner,
                    'name': member.get('name') 
                } 
            }) %}
            {% endif %}
        {% endfor %}
    {% endfor %}
    {{ return (owners) }}
{% endmacro %}