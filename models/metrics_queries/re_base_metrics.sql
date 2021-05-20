-- depends_on: {{ ref('re_monitored_columns') }}
-- depends_on: {{ ref('re_monitored_tables') }}

{%- set tables =  run_query(get_tables()) %}
{%- set table_results = [] %}

{%- for mtable in tables %}
    {% set table_name = row_value(mtable, 'table_name') %}
    {% set time_filter = row_value(mtable, 'time_filter') %}

    {%- call statement('metrics', fetch_result=True) -%}
    select
        {{- base_metrics_query(mtable) -}}
    from
        {{table_name}}
    where
        {{time_filter}} >= {{ time_window_start() }} and
        {{time_filter}} < {{ time_window_end() }}
    {%- endcall -%}

    {%- set result = load_result('metrics')['table'] -%}
    {%- do table_results.append({'table': table_name, 'result': result}) %}
{% endfor %}

{%- for result in table_results %}
    {%- set table_name = result.table %}
    {%- set m_for_table = result.result %}
    {%- for column in m_for_table.columns %}
        {%- set column_value = column.values()[0] %}
        {%- set column_name = column.name %}
        {%- set table_column_name, fun = column_name.split('___') %}

        select 
            '{{table_name}}' as table_name,
            '{{table_column_name}}' as column_name,
            '{{fun}}' as metric,
            {{column_value | replace(None, 'null::integer')}} as value,
            {{- time_window_start() -}} as time_window_start,
            {{- time_window_end() -}} as time_window_end,
            {{- dbt_utils.current_timestamp_in_utc() -}} as computed_on
        {%- if not loop.last %} union all {%- endif %}
        {% endfor %}

    {%- if not loop.last %} union all {%- endif %}
{%- endfor %}


