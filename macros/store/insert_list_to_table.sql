

{% macro insert_list_to_table(table, list, insert_size=100) %}

    {% set single_insert_list = [] %}
    {% set cols = adapter.get_columns_in_relation(table) %}
    {% for el in list %}
        {% do single_insert_list.append(el) %}
        {% set single_insert_list_size = single_insert_list | length %}
        {% if single_insert_list_size == insert_size or loop.last %}

            {% set insert_query %}
                insert into {{ table }} ({%- for c in cols %}{{ c.name }}{% if not loop.last %}, {% endif %}{% endfor %}) values
                {%- for row in single_insert_list -%}
                    (
                    {%- for c in cols -%}
                        {% set col_name = c.name | lower %}
                        {%- if row[col_name] is none -%}
                            NULL
                        {%- else -%}
                            {%- if row[col_name] is string -%}
                                '{{ re_data.quote_constant(row[col_name]) }}'
                            {%- elif row[col_name] is number -%}
                                {{- row[col_name] -}}
                            {%- else -%}
                                '{{- re_data.quote_constant(tojson(row[col_name])) -}}'
                            {%- endif -%}
                        {%- endif -%}
                        {%- if not loop.last -%},{%- endif -%}
                    {%- endfor -%}
                    )
                    {%- if not loop.last -%},{%- endif %}
                {% endfor -%}
            {% endset %}
            
            {% call statement('insert_results',fetch_result=True, auto_begin=True) %}
              {{ insert_query }}; commit
            {% endcall %}

            {% do single_insert_list.clear() %}

        {% endif %}
    {% endfor %}

{% endmacro %}