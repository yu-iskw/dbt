

{% macro get_merge_sql(target, source, unique_key, dest_columns, partition_by) -%}
  {{ adapter_macro('get_merge_sql', target, source, unique_key, dest_columns, partition_by) }}
{%- endmacro %}

{% macro get_delete_insert_merge_sql(target, source, unique_key, dest_columns) -%}
  {{ adapter_macro('get_delete_insert_merge_sql', target, source, unique_key, dest_columns) }}
{%- endmacro %}


{% macro get_quoted_csv(column_names) %}
    {% set quoted = [] %}
    {% for col in column_names -%}
        {%- do quoted.append(adapter.quote(col)) -%}
    {%- endfor %}

    {%- set dest_cols_csv = quoted | join(', ') -%}
    {{ return(dest_cols_csv) }}
{% endmacro %}

{% macro common_get_merge_sql(target, source, unique_key, dest_columns, partition_by) -%}
    {%- set dest_cols_csv =  get_quoted_csv(dest_columns | map(attribute="name")) -%}

    {%- set conditions = [] -%}

    {% if unique_key %}
        {% set unique_key_match %}
            DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
        {% endset %}
        {% do conditions.append(unique_key_match) %}
    {% endif %}
    
    {# BigQuery only #}
    {%- if partition_by -%}
        {% set dest_partition_filter %}
            {{pprint_partition_field(partition_by, alias = 'DBT_INTERNAL_DEST')}} in unnest(partitions_for_upsert)
        {% endset %}
        {%- do conditions.append(dest_partition_filter) -%}
    {% endif %}

    merge into {{ target }} as DBT_INTERNAL_DEST
        using {{ source }} as DBT_INTERNAL_SOURCE
        on
        {% if conditions|length == 0 %}
            FALSE
        {% else %}
            {{ conditions | join(' and ') }}
        {% endif %}

    {% if unique_key %}
    when matched then update set
        {% for column in dest_columns -%}
            {{ adapter.quote(column.name) }} = DBT_INTERNAL_SOURCE.{{ adapter.quote(column.name) }}
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
    {% endif %}

    when not matched then insert
        ({{ dest_cols_csv }})
    values
        ({{ dest_cols_csv }})

{%- endmacro %}

{% macro default__get_merge_sql(target, source, unique_key, dest_columns, partition_by) -%}
    {% set typename = adapter.type() %}

    {{ exceptions.raise_compiler_error(
        'get_merge_sql is not implemented for {}'.format(typename)
       )
    }}

{% endmacro %}


{% macro common_get_delete_insert_merge_sql(target, source, unique_key, dest_columns) -%}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    {% if unique_key is not none %}
    delete from {{ target }}
    where ({{ unique_key }}) in (
        select ({{ unique_key }})
        from {{ source }}
    );
    {% endif %}

    insert into {{ target }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ source }}
    );

{%- endmacro %}

{% macro default__get_delete_insert_merge_sql(target, source, unique_key, dest_columns) -%}
    {{ common_get_delete_insert_merge_sql(target, source, unique_key, dest_columns) }}
{% endmacro %}
