{% macro in_compile() %}

    {%- call statement('in_compile', fetch_result=True) -%}
        select * from {{ ref('re_data_run_started_at') }}
    {%- endcall -%}

    {% if execute %}
        {%- set result = load_result('in_compile')['data'][0][0] -%}
        {% if result == run_started_at.timestamp() * 1000000 %}
            {{ return(False) }}
        {% else %}
            {{ return(True) }}
        {% endif %}
    {% endif %}

{% endmacro %}