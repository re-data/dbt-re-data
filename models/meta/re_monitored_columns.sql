{% set schemas = var('re_data:schemas') %}

{% for for_schema in schemas %}
    {{ get_monitored_columns(for_schema) }}
{%- if not loop.last %} union all {%- endif %}
{% endfor %}