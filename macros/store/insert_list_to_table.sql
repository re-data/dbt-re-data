{% macro insert_list_to_table(table, list, params, insert_size=100) %}

    {% set single_insert_list = [] %}
    {% for el in list %}
        {% do single_insert_list.append(el) %}
        {% set single_insert_list_size = single_insert_list | length %}
        {% if single_insert_list_size == insert_size or loop.last %}

            {% set insert_query %}
                insert into {{ table }} ({%- for p in params %}{{p}}{% if not loop.last %}, {% endif %}{% endfor %}) values
                {%- for row in single_insert_list -%}
                    (
                    {%- for p in params -%}
                        {%- if row[p] is none -%}
                            NULL
                        {%- else -%}
                            {%- if row[p] is string -%}
                                {{- re_data.quote_string(row[p]) -}}
                            {%- elif row[p] is number -%}
                                {{-row[p]-}}
                            {%- else -%}
                                {{- re_data.quote_string(tojson(row[p])) -}}
                            {%- endif -%}
                        {%- endif -%}
                        {%- if not loop.last -%},{%- endif -%}
                    {%- endfor -%}
                    )
                    {%- if not loop.last -%},{%- endif %}
                {% endfor -%}
            {% endset %}

            {% do run_query(insert_query) %}
            {% do single_insert_list.clear() %}
        {% endif %}
    {% endfor %}

{% endmacro %}