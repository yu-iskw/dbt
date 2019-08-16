
{% macro generate_query_header(node) -%}

    {% set payload = {
        "app": "dbt",
        "dbt_version": dbt_version,
        "target_name": target.name,
        "user": target.user | default("unset"),

        "file": node.original_file_path | default("unset"),
        "node_id": node.unique_id,
        "node_name": node.name,
        "resource_type": node.resource_type,
        "package_name": node.package_name,
        "tags": node.config.get('tags'),

        "relation": node.database ~ "." ~ node.schema ~ "." ~ node.alias,
        "database": node.database,
        "schema": node.schema,
        "identifier": node.alias,
    } %}

    /* dbt query context: {{ tojson(payload) }} */

{%- endmacro %}
