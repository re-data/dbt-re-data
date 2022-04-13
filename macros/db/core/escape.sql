
{% macro escape_seq_for_json(chr) %}{{adapter.dispatch('escape_seq_for_json', 're_data')(chr)}}{% endmacro %}

{% macro default__escape_seq_for_json(chr) %}'\\\{{chr}}'{% endmacro %}