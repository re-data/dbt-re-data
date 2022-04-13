
{% macro metric_expression(table, metric, expression, column_name=None, condition=None) %}
    select * from {{ref('re_data_base_metrics')}}
    where
        table_name = '{{ re_data.full_table_name_values(table.identifier, table.schema, table.database)}}' and
        metric = '{{ metric }}' and
        {% if condition is not none %}
            {{ condition }} and
        {% endif %}
        {% if column_name is none %}
        not ( {{ expression }} )
        {% else %}
        column_name = '{{ column_name }}' and
        not ( {{ expression }} )
        {% endif %}

{% endmacro %}

{# old test macros, will be removed after some time #}
{% test metric_expression_is_true(model, table, metric, expression, column_name=None, condition=None) %}
    {{ re_data.metric_expression(table, metric, expression, column_name=None, condition=None) }}
{% endtest %}


{% test metric_equal_to(model, table, metric, value, column_name=None, condition=None) %}
    {{ re_data.metric_expression(table, metric, 'value = ' ~ value, column_name, condition) }}
{% endtest %}


{% test metric_in_range(model, table, metric, min_value, max_value, column_name=None, condition=None) %}
    {{ re_data.metric_expression(table, metric, 'value >= ' ~ min_value ~ ' and value <= ' ~ max_value, column_name, condition) }}
{% endtest %}

{# new test macros #}

{% test assert_true(model, column_name=None, metric=None, expression=expression, where=None) %}
    -- depends_on: {{ ref('re_data_base_metrics') }}
    {% if execute %}
        {{ re_data.metric_expression(model, metric, expression, column_name, where) }}
    {% else %}
        {{ re_data.empty_table() }}
    {% endif %}
{% endtest %}

{% test assert_false(model, column_name=None, metric=None, expression=expression, where=None) %}
    -- depends_on: {{ ref('re_data_base_metrics') }}
    {% if execute %}
        {{ re_data.metric_expression(model, metric, 'not (' ~ expression ~ ')', column_name, where) }}
    {% else %}
        {{ re_data.empty_table() }}
    {% endif %}
{% endtest %}

{% test assert_in_range(model, column_name=None, metric=None, min_value=None, max_value=None, where=None) %}
    -- depends_on: {{ ref('re_data_base_metrics') }}
    {% if execute %}
        {{ re_data.metric_expression(model, metric, 'value >= ' ~ min_value ~ ' and value <= ' ~ max_value, column_name, where) }}
    {% else %}
        {{ re_data.empty_table() }}
    {% endif %}
{% endtest %}

{% test assert_equal(model, column_name=None, metric=None, value=value, where=None) %}
    -- depends_on: {{ ref('re_data_base_metrics') }}
    {% if execute %}
        {{ re_data.metric_expression(model, metric, 'value = ' ~ value, column_name, where) }}
    {% else %}
        {{ re_data.empty_table() }}
    {% endif %}
{% endtest %}

{% test assert_greater(model, column_name=None, metric=None, value=None, where=None) %}
    -- depends_on: {{ ref('re_data_base_metrics') }}
    {% if execute %}
        {{ re_data.metric_expression(model, metric, 'value > ' ~ value, column_name, where) }}
    {% else %}
        {{ re_data.empty_table() }}
    {% endif %}
{% endtest %}

{% test assert_greater_equal(model, column_name=None, metric=None, value=None, where=None) %}
    -- depends_on: {{ ref('re_data_base_metrics') }}
    {% if execute %}
        {{ re_data.metric_expression(model, metric, 'value >= ' ~ value, column_name, where) }}
    {% else %}
        {{ re_data.empty_table() }}
    {% endif %}
{% endtest %}

{% test assert_less(model, column_name=None, metric=None, value=None, where=None) %}
    -- depends_on: {{ ref('re_data_base_metrics') }}
    {% if execute %}
        {{ re_data.metric_expression(model, metric, 'value < ' ~ value, column_name, where) }}
    {% else %}
        {{ re_data.empty_table() }}
    {% endif %}
{% endtest %}

{% test assert_less_equal(model, column_name=None, metric=None, value=None, where=None) %}
    -- depends_on: {{ ref('re_data_base_metrics') }}
    {% if execute %}
        {{ re_data.metric_expression(model, metric, 'value <= ' ~ value, column_name, where) }}
    {% else %}
        {{ re_data.empty_table() }}
    {% endif %}
{% endtest %}