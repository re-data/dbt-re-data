{% macro is_datetime(column) %}
    {{ adapter.dispatch('is_datetime', 're_data')(column) }}
{% endmacro %}

{% macro default__is_datetime(column) %}
    case when {{column}} in (
            'timestamp without time zone',
            'timestamp with time zone',
            'date'
    )
        then true
    else
        false
    end

{% endmacro %}

{% macro bigquery__is_datetime(column) %}
    case when {{column}} in (
            'DATE',
            'DATETIME',
            'TIMESTAMP'
    )
        then true
    else
        false
    end
{% endmacro %}

{% macro snowflake__is_datetime(column) %}
    case when {{ column }} in (
        'DATE',
        'DATETIME',
        'TIME',
        'TIMESTAMP',
        'TIMESTAMP_LTZ',
        'TIMESTAMP_NTZ',
        'TIMESTAMP_TZ'
    )
        then true
    else
        false
    end

{% endmacro %}


{% macro get_column_type(column) %}
    {% set result = adapter.dispatch('get_column_type', 're_data')(column) %}
    {{ return(result) }}
{% endmacro %}


{% macro default__get_column_type(column) %}
    
    {% if column.data_type in [
        'character varying',
        'varchar',
        'character',
        'char',
        'text'
    ] %}
        {{ return('text') }}

    {% elif column.data_type in [
            'smallint',
            'integer',
            'bigint',
            'decimal',
            'numeric',
            'real',
            'double precision',
            'enum',
        ] %}
        {{ return('numeric') }}

    {% else %}
        {{ return('unknown') }}

    {% endif %}

{% endmacro %}


{% macro snowflake__get_column_type(column) %}

    {% if column.DATA_TYPE in [
        'VARCHAR',
        'CHAR',
        'CHARACTER',
        'STRING',
        'TEXT'
    ] %}

        {{ return('text') }}

    {% elif column.DATA_TYPE in [
            'NUMBER',
            'DECIMAL',
            'NUMERIC',
            'INT',
            'INTEGER',
            'BIGINT',
            'SMALLINT',
            'TINYINT',
            'BYTEINT',
            'FLOAT',
            'FLOAT4',
            'FLOAT8',
            'DOUBLE',
            'DOUBLE PRECISION',
            'REAL',
    ] %}

        {{ return('numeric') }}

    {% else %}

        {{ return('unknown') }}

    {% endif %}

{% endmacro %}


{% macro bigquery__get_column_type(column) %}
    
    {% if column.data_type in [
        'STRING'
    ] %}

        {{ return('text') }}

    {% elif column.data_type in [
        "INT64", "NUMERIC", "BIGNUMERIC", "FLOAT64", "INTEGER"]
    %}

        {{ return('numeric') }}

    {% else %}
    
        {{ return('unknown') }}

    {% endif %}

{% endmacro %}