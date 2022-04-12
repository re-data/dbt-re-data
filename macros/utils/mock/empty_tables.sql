
{% macro all_types_select() %}
    with types_table as (
        select
            cast (null as {{ string_type() }}) as string_type,
            cast (null as {{ long_string_type() }}) as long_string_type,
            cast (1 as {{ numeric_type() }}) as numeric_type,
            cast ('2000-01-10' as {{ timestamp_type() }}) as timestamp_type,
            cast (true as {{ boolean_type() }}) as boolean_type
    )
{% endmacro %}

{% macro empty_table_generic(list) %}
    {{ re_data.all_types_select() }}
    select
    {% for name, type in list %}
         {{ type }}_type as {{ name }}
        {%- if not loop.last %}, {%- endif %}
    {% endfor %}
    from types_table
    where string_type is not null
{% endmacro %}

{% macro empty_last_base_metrics() %}
    {{
        re_data.empty_table_generic([
            ('table_name', 'string'),
            ('column_name', 'string'),
            ('metric', 'string'),
            ('value', 'numeric')
        ])
    }}
{% endmacro %}

{% macro empty_columns_table() %}
    {{
        re_data.empty_table_generic([
            ('name', 'string'),
            ('schema', 'string'),
            ('database', 'string'),
            ('column_name', 'string'),
            ('data_type', 'string'),
            ('is_nullable', 'boolean'),
            ('time_filter', 'string'),
            ('computed_on', 'timestamp')
        ])
    }}
{% endmacro %}


{% macro empty_table() %}
    {{
        re_data.empty_table_generic([
            ('name', 'string')
        ])
    }}
{% endmacro %}

