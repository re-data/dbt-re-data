

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