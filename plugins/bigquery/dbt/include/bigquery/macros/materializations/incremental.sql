
{% materialization incremental, adapter='bigquery' -%}

  {%- set unique_key = config.get('unique_key') -%}
  {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}

  {%- set target_relation = this %}
  {%- set existing_relation = load_relation(this) %}
  {%- set tmp_relation = make_temp_relation(this) %}

  {%- set partition_by = config.get('partition_by', none) -%}
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
      {% if not adapter.is_replaceable(existing_relation, partition_by, cluster_by) %}
          {% do log("Hard refreshing " ~ existing_relation ~ " because it is not replaceable") %}
          {{ adapter.drop_relation(existing_relation) }}
      {% endif %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% else %}
     {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
     
     {# If partitioned, get the earliest partition with updates #}
     {% if partition_by %}
         {%- call statement('pro_tmp', fetch_result = True) -%}
            {{ create_table_as(True, tmp_relation, sql) }}
         {%- endcall -%}

         {% call statement('get_min_source_partition', fetch_result = True) %}
            select min({{partition_by}}), max({{partition_by}}) from {{tmp_relation}}
         {% endcall %}
         
         {% set partition_range = load_result('get_min_source_partition').data[0]|list %}
         {% set partition_min, partition_max = partition_range[0]|string, partition_range[1]|string %}
      {% endif %}

     {#-- wrap sql in parens to make it a subquery --#}
     {% set source_sql -%}
       (
         {{ sql }}
       )
     {%- endset -%}
     
     {%- set dest_partition = {
        'name': partition_by|lower|replace('date(','')|replace(')',''), 
        'min': partition_min, 
        'max': partition_max
        } -%}
     
     {% set build_sql = get_merge_sql(target_relation, source_sql, unique_key, dest_columns, dest_partition) %}
  {% endif %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {% endcall %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
