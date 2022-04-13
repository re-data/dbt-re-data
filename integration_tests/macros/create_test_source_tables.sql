

{% macro create_test_source_tables() %}

    {% set create_table %}
        CREATE SCHEMA IF NOT EXISTS {{target.schema}};
        DROP TABLE IF EXISTS {{target.schema}}.re_data_source_test_table;
        CREATE TABLE IF NOT EXISTS {{target.schema}}.re_data_source_test_table (
            number {{ re_data.integer_type() }},
            description {{ re_data.string_type() }},
            created_at {{ re_data.timestamp_type() }}
        );
        INSERT INTO {{target.schema}}.re_data_source_test_table (number, description, created_at) VALUES 
            (1, 'one', current_timestamp),
            (2, 'two', current_timestamp),
            (3, 'three', current_timestamp),
            (4, 'four', current_timestamp),
            (5, 'five', current_timestamp),
            (6, 'six', current_timestamp),
            (7, 'seven', current_timestamp),
            (8, 'eight', current_timestamp),
            (9, 'nine', current_timestamp),
            (10, 'ten', current_timestamp
        );
    {% endset %}
    {% do run_query(create_table) %}

{% endmacro %}