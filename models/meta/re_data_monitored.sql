{{
    config(
        materialized='table',
        unique_key = 'table_name',
        post_hook="{% if execute %}{{ pub_insert_into_re_data_monitored() }}{% endif %}"
    )
}}

{{
    re_data.empty_table_generic([
        ('name', 'string'),
        ('schema', 'string'),
        ('database', 'string'),
        ('time_filter', 'string'),
        ('metrics', 'string'),
        ('columns', 'string'),
        ('anomaly_detector', 'string'),
        ('owners', 'string')
    ])
}}