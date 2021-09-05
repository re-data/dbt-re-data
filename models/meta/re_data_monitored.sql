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
    {% for table_spec in var('re_data:tables') %}
        {% do code_list.extend(get_spec(table_spec)) %}
    {% endfor %}

    {% if code_list != [] %}
        {% for el in code_list %}
            select {{el['table_name'], el['time_filter'], el['actively_monitored'] }}
            {% if not loop.last %} union all {% endif %}
        {% endfor %}
    {% else %}
        {{ re_data.empty_code_monitored() }}
    {% endif %}
),

all_status as (
select 
    dm.table_name,
    case 
        when cm.time_filter is not null
                then cm.time_filter
             else dm.time_filter
    end as time_filter,
    case 
        when cm.actively_monitored is not null
                then cm.actively_monitored
             else dm.actively_monitored
    end as actively_monitored
from
    db_monitored dm left join code_monitored cm
    on dm.table_name = cm.table_name
)

select table_name, time_filter
from all_status where
    actively_monitored = true
    and time_filter is not null

{% endif %}

