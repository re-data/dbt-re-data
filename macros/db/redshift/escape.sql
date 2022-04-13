
{% macro redshift__escape_seq_for_json(chr) %}'\\\{{chr}}'{% endmacro %}