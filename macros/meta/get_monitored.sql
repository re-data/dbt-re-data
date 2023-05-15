{% macro pub_monitored_from_graph() %}
    {% set monitored = [] %}
    {% set both = []%}
    {% do both.extend(graph.nodes.values()) %}
    {% do both.extend(graph.sources.values()) %}
    {% set owners_config = re_data.get_owners_config() %}

    {% set select_var = var('re_data:select') %}
    {% set select_all = true %}

    {% set selected_nodes = none %}
    {% set selected_tags = none %}

    {% if select_var is not none %}
        {% set select_all = false %}
        {% set selected_nodes = dict() %}
        {% set selected_tags = dict() %}

        {% for el in select_var %}
            {% if el.startswith('tag:') %}
                {% do selected_tags.update({el[4:]: True}) %}
            {% else %}
                {% do selected_nodes.update({el: True}) %}
            {% endif %}
        {% endfor %}
    {% endif %}

    {% for el in both %}
        {% if el.resource_type in ['model', 'seed', 'source'] %}
            {% if el.config.get('re_data_monitored') %}
                {% set target_name = el.identifier or el.alias or el.name %}

                {% if select_all %}
                    {% set selected = true %}
                {% else %}
                    {% set selected_name = selected_nodes.get(target_name, false) %}
                    {% set selected_tag = [] %}

                    {% for tag in el.tags %}
                        {% if selected_tags.get(tag, false) %}
                            {% do selected_tag.append(true) %}
                        {% endif %}
                    {% endfor %}
                    
                    {% set selected = selected_name or (selected_tag | length > 0) %}
                {% endif %}

                {% set metrics_groups = el.config.get('re_data_metrics_groups', var('re_data:default_metrics')) %}
                {% set additional_metrics = el.config.get('re_data_metrics', {}) %}

                {% do monitored.append({
                    'name': re_data.name_in_db(target_name),
                    'schema': re_data.name_in_db(el.schema),
                    'database': re_data.name_in_db(el.database),
                    'time_filter': el.config.get('re_data_time_filter', none),
                    'metrics_groups': metrics_groups,
                    'additional_metrics': re_data.metrics_in_db(additional_metrics),
                    'metrics': re_data.metrics_in_db(re_data.final_metrics(metrics_groups, additional_metrics)),
                    'columns': re_data.columns_in_db(el.config.get('re_data_columns', none)),
                    'anomaly_detector': el.config.get('re_data_anomaly_detector', var('re_data:anomaly_detector', {})),
                    'owners': re_data.prepare_model_owners(el.config.get('re_data_owners', []), owners_config),
                    'selected': selected
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