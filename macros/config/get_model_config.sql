
{% macro get_model_config(monitored) %}
    {% set model = {} %}
    {% do model.update({'name': re_data.row_value(monitored, 'name')}) %}
    {% do model.update({'schema': re_data.row_value(monitored, 'schema')}) %}
    {% do model.update({'database': re_data.row_value(monitored, 'database')}) %}
    {% do model.update({'time_filter': re_data.row_value(monitored, 'time_filter')}) %}    
    {% do model.update({'metrics': fromjson(re_data.row_value(monitored, 'metrics'))}) %}
    {% do model.update({'model_name': model.get('database') + '.' + model.get('schema') + '.' + model.get('name')}) %}
    {% do model.update({'table_name': full_table_name_values(model.get('name'), model.get('schema'), model.get('database'))}) %}

    {% set columns_db = re_data.row_value(monitored, 'columns') %}

    {% set column_list = fromjson(columns_db) if columns_db is not none else none %}
    {% set columns_dict = re_data.dict_from_list(column_list) %}

    {% do model.update({'columns_dict': columns_dict}) %}
    {% do model.update({'columns_compute_all': columns_dict is none}) %}

    {% set columns_query %}
        select * from {{ ref('re_data_columns') }}
        where name = '{{ model.name }}' and schema = '{{ model.schema }}' and database = '{{ model.database }}'
    {% endset %}
    {% set columns = run_query(columns_query) %}

    {% do model.update({'columns': columns}) %}/

    {{ return(model) }}
{% endmacro %}

{% macro should_compute_metric(model, column_name) %}
    {{ return(model.columns_compute_all or model.columns_dict.get(column_name)) }}
{% endmacro %}