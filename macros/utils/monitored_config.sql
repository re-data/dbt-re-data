

{% macro columns_in_db(columns) %}
    {% set translated = [] %}
    {% for col in columns %}
        {% do translated.append(re_data.name_in_db(col))%}
    {% endfor %}
    {{ return (translated) }}
{% endmacro %}

{% macro metrics_in_db(metrics) %}
    {% set translated = metrics %}
    {% set column_metrics = {} %}
    {% for col in metrics.column %}
        {% do column_metrics.update({re_data.name_in_db(col): metrics.column[col]}) %}
    {% endfor %}
    {% if column_metrics %}
        {% do metrics.update({'column': column_metrics}) %}
    {% endif %}
    {{ return (metrics) }}
{% endmacro %}

{% macro final_metrics(metrics_groups, additional_metrics) %}
    {% set final_metrics_dict = dict([('group', {}), ('additional', {})]) %}
    {% set all_metrics_groups = var('re_data:metrics_groups')%}

    {% for group in metrics_groups %}
        {% set value = all_metrics_groups.get(group) %}
        {% do final_metrics_dict['group'].update(value) %}
    {% endfor %}

    {% do final_metrics_dict['additional'].update(additional_metrics) %}
    {{ return (final_metrics_dict) }}

{% endmacro %}