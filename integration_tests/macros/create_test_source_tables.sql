

{% macro create_test_source_tables() %}

    DROP TABLE IF EXISTS {{target.schema}}.re_data_source_test_table;
    CREATE TABLE IF NOT EXISTS {{target.schema}}.re_data_source_test_table (
        number {{ re_data.integer_type() }},
        description {{ re_data.string_type() }},
        created_at {{ re_data.timestamp_type() }}
    );
    INSERT INTO {{target.schema}}.re_data_source_test_table (number, description, created_at) VALUES 
        (1, 'one', now()),
        (2, 'two', now()),
        (3, 'three', now()),
        (4, 'four', now()),
        (5, 'five', now()),
        (6, 'six', now()),
        (7, 'seven', now()),
        (8, 'eight', now()),
        (9, 'nine', now()),
        (10, 'ten', now()
    );

{% endmacro %}