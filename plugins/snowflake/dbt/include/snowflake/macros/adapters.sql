{% macro snowflake__create_table_as(temporary, relation, sql) -%}
  {%- set transient = config.get('transient', default=true) -%}
  {%- set cluster_by_keys = config.get('cluster_by', default=none) -%}
  {%- set enable_automatic_clustering = config.get('automatic_clustering', default=false) -%}
  {%- set copy_grants = config.get('copy_grants', default=false) -%}
  {%- if cluster_by_keys is not none and cluster_by_keys is string -%}
    {%- set cluster_by_keys = [cluster_by_keys] -%}
  {%- endif -%}
  {%- if cluster_by_keys is not none -%}
    {%- set cluster_by_string = cluster_by_keys|join(", ")-%}
  {% else %}
    {%- set cluster_by_string = none -%}
  {%- endif -%}

      create or replace {% if temporary -%}
        temporary
      {%- elif transient -%}
        transient
      {%- endif %} table {{ relation }} {% if copy_grants and not temporary -%} copy grants {%- endif %} as
      (
        {%- if cluster_by_string is not none -%}
          select * from(
            {{ sql }}
            ) order by ({{ cluster_by_string }})
        {%- else -%}
          {{ sql }}
        {%- endif %}
      );
    {% if cluster_by_string is not none and not temporary -%}
      alter table {{relation}} cluster by ({{cluster_by_string}});
    {%- endif -%}
    {% if enable_automatic_clustering and cluster_by_string is not none and not temporary  -%}
      alter table {{relation}} resume recluster;
    {%- endif -%}

{% endmacro %}

{% macro snowflake__create_view_as(relation, sql) -%}
  {%- set secure = config.get('secure', default=false) -%}
  {%- set copy_grants = config.get('copy_grants', default=false) -%}
  create or replace {% if secure -%}
    secure
  {%- endif %} view {{ relation }} {% if copy_grants -%} copy grants {%- endif %} as (
    {{ sql }}
  );
{% endmacro %}

{% macro snowflake__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
    show columns in {{ relation }};
    select
        column_name,
        data_type,
        character_maximum_length,
        numeric_precision,
        numeric_scale
    from (
        select
            "column_name" as column_name,
            parse_json("data_type") as ttype, -- ignored, used as a lateral alias
            ttype:type::string as data_type, -- TODO
            case
                when ttype:type::string = 'TEXT' then ttype:length::int
                else null::int
            end as character_maximum_length,
            case
                when ttype:type::string = 'FIXED' then ttype:precision::int
                else null::int
            end as numeric_precision,
            case
                when ttype:type::string = 'FIXED' then ttype:scale::int
                else null::int
            end as numeric_scale
        from table(result_scan(last_query_id()))
    );
  {% endcall %}

  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}

{% endmacro %}


{% macro snowflake__list_relations_without_caching(information_schema, schema) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    show terse objects in {{ information_schema.database }}.{{ schema }};
    select
        "database_name" as database_name,
        "name" as name,
        "schema_name" as schema,
        case when "kind" = 'TABLE' then 'table'
             when "kind" = 'VIEW' then 'view'
             else "kind"
        end as table_type
    from table(result_scan(last_query_id()));
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}


{% macro snowflake__check_schema_exists(information_schema, schema) -%}
  {% call statement('check_schema_exists', fetch_result=True) -%}
    show schemas like '{{ schema }}' in database {{ information_schema.database }};
    select count(*)
    from table(result_scan(last_query_id()));
  {%- endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{%- endmacro %}

{% macro snowflake__list_schemas(database) -%}
  {% set sql %}
    show schemas in database {{ database }};
    select distinct "name" as schema_name
    from table(result_scan(last_query_id()));
  {% endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}

{% macro snowflake__current_timestamp() -%}
  convert_timezone('UTC', current_timestamp())
{%- endmacro %}

{% macro snowflake__snapshot_get_time() -%}
  to_timestamp_ntz({{ current_timestamp() }})
{%- endmacro %}


{% macro snowflake__rename_relation(from_relation, to_relation) -%}
  {% call statement('rename_relation') -%}
    alter table {{ from_relation }} rename to {{ to_relation }}
  {%- endcall %}
{% endmacro %}


{% macro snowflake__alter_column_type(relation, column_name, new_column_type) -%}
  {% call statement('alter_column_type') %}
    alter table {{ relation }} alter {{ adapter.quote(column_name) }} set data type {{ new_column_type }};
  {% endcall %}
{% endmacro %}
