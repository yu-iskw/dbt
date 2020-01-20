
{% materialization incremental, adapter='bigquery' -%}

  {%- set unique_key = config.get('unique_key') -%}
  {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}

  {%- set target_relation = this %}
  {%- set existing_relation = load_relation(this) %}
  {%- set tmp_relation = make_temp_relation(this) %}

  {%- set partition_by = parse_partition_by(config.get('partition_by', none)) -%}
  {%- set cluster_by = config.get('cluster_by', none) -%}
  {%- set min_source_partition = none -%}

  {{ run_hooks(pre_hooks) }}

  {% if existing_relation is none %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif existing_relation.is_view %}
      {#-- There's no way to atomically replace a view with a table on BQ --#}
      {{ adapter.drop_relation(existing_relation) }}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif full_refresh_mode %}
      {#-- If the partition/cluster config has changed, then we must drop and recreate --#}
      {% if not adapter.is_replaceable(existing_relation, partition_by.name, cluster_by) %}
          {% do log("Hard refreshing " ~ existing_relation ~ " because it is not replaceable") %}
          {{ adapter.drop_relation(existing_relation) }}
      {% endif %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% else %}
     {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
     
     {%- set dest_partition = none -%}
     
     {#-- if partitioned, use BQ scripting to get the range of partition values to be updated --#}
     {% if partition_by %}
        
        {% set build_sql %}
        
        DECLARE
            partitions_for_upsert array<{{partition_by.data_type}}>;

        -- create temporary table
        {{ create_table_as('scripting', tmp_relation, sql) }}

        SET (partitions_for_upsert) = (
            select as struct
                array_agg(distinct {{cast_to_date(partition_by)}})
            from {{tmp_relation.identifier}}
        );
        {%- set source_sql -%}
        (
          select * from {{tmp_relation.identifier}}
        )
        {%- endset -%}
        
        {{ get_merge_sql(target_relation, source_sql, unique_key, dest_columns, partition_by) }}
        
        {% endset %}
          
      {% else %}
      
          {#-- wrap sql in parens to make it a subquery --#}
          {%- set source_sql -%}
            (
              {{sql}}
            )
          {%- endset -%}
          
          {% set build_sql = get_merge_sql(target_relation, source_sql, unique_key, dest_columns, partition_by) %}
          
      {% endif %}
     
  {% endif %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {% endcall %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
