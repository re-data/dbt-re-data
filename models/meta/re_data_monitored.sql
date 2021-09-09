{{
    config(
        materialized='table',
        unique_key = 'table_name'
    )
}}
{% if not execute %}
    select * from  {{ ref('re_data_tables') }}

{% else %}

with db_monitored as (
    select
        table_name, time_filter, actively_monitored
    from 
        {{ ref('re_data_tables') }}
)
, code_monitored as (
    {% set code_list = [] %}

    {% set my_context = context[project_name] %}
    {% for group in var('re_data:monitored') %}
        {% for table in group['tables'] %}
            {% do code_list.extend(re_data.monitoring_spec(table, group)) %}
        {% endfor %}
    {% endfor %}

    {% if code_list != [] %}
        {% for el in code_list %}
            select {{- full_table_name_values(el.table, el.schema, el.database) -}} as table_name,
                   cast( {{- str_or_null(el.time_filter) -}} as {{ string_type() }} ) as time_filter,
                   cast( {{- bool_or_null(el.actively_monitored) }} as {{boolean_type()}} ) as actively_monitored,
                   '{{ tojson(el.metrics) }}' as metrics
            {% if not loop.last %} union all {% endif %}
        {% endfor %}
    {% else %}
        {{ re_data.empty_code_monitored() }}
    {% endif %}
),

all_status as (
select 
    dm.table_name as table_name,
    case 
        when cm.time_filter is not null
                then cm.time_filter
             else dm.time_filter
    end as time_filter,
    case 
        when cm.actively_monitored is not null
                then cm.actively_monitored
             else dm.actively_monitored
    end as actively_monitored,
    cm.metrics as metrics
from
    db_monitored dm left join code_monitored cm
    on dm.table_name = cm.table_name
)

select table_name, time_filter, metrics
from all_status where
    actively_monitored = true
    and time_filter is not null

{% endif %}

