{% macro bigquery__get_merge_sql(target, source, unique_key, dest_columns, dest_partition) %}
    {{ common_get_merge_sql(target, source, unique_key, dest_columns, dest_partition) }}
{% endmacro %}
