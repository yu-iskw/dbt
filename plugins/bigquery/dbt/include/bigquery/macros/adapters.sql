{# call this macro first, assume new spec thereafter #}
{%- macro parse_partition_by(raw_partition_by) -%}
    {# check for new partition_by spec #}
    {% if raw_partition_by is mapping %}
        {{return(raw_partition_by)}}
    {# otherwise, see what kind of string is being passed #}
    {% elif raw_partition_by is string %}
        {% if 'range_bucket' in raw_partition_by|lower %}
            {# if integer-range (beta), we have no hope of parsing #}
            {{ exceptions.raise_catompiler_error(
                "BigQuery integer-range partitioning (currently in beta) is supported
                by the new `partition_by` config, which expects a dictionary. See:
                    latest dbt docs link"
            ) }}
        {% else %}
            {# if date or timestamp, raise deprecation warning and *try* to parse #}
            {% set p = modules.re.compile(
                '([ ]?date[ ]?\([ ]?)?([\`\w]+)(?:[ ]?\)[ ]?)?',
                modules.re.IGNORECASE) %}
            {% set m = p.match(raw_partition_by) %}
            {% set inferred_partition_by = {
                'name': m.group(2),
                'data_type': 'timestamp' if ('date' in m.group(1)|lower) else 'date'
            } %}
            {% set deprecation_warning %}
                Deprecation warning: as of dbt version 0.16.0, the `partition_by`
                config on BigQuery now expects a dictionary. This will cause an error
                in a future release.
                
                You supplied: {{raw_partition_by}}
                dbt inferred: {{inferred_partition_by}}
            {% endset %}            
            {% do log(deprecation_warning, info = true) %}
            {{return(inferred_partition_by)}}
        {% endif %}
    {% else %}
        {{return(none)}}
    {% endif %}
{%- endmacro -%}

{% macro partition_by(partition_by_dict) %}
    {%- set partition_by_type = partition_by_dict.data_type|trim|lower -%}
    {%- if partition_by_type == 'date' -%}
        partition by {{ partition_by_dict.name }}
    {%- elif partition_by_type in ('timestamp','datetime') -%}
        partition by date({{ partition_by_dict.name }})
    {%- elif partition_by_type in ('integer','int64') -%}
        {%- set pbr = partition_by_dict.range -%}
        partition by range_bucket(
            {{partition_by_dict.name}},
            generate_array({{pbr.start}}, {{pbr.end}}, {{pbr.interval}})
        )
    {%- endif -%}
{%- endmacro -%}

{% macro cast_to_date(partition_by_dict, alias = '') %}

    {%- set partition_col_exp -%}
        {%- if alias -%} `{{alias}}`.`{{partition_by_dict.name}}`
        {%- else -%} `{{partition_by_dict.name}}`
        {%- endif -%}
    {%- endset -%}

    {%- if partition_by_dict.type in ('timestamp','datetime') -%}
        date({{partition_col_exp}})
    {%- else -%}
        {{partition_col_exp}}
    {%- endif -%}

{% endmacro %}


{% macro cluster_by(raw_cluster_by) %}
  {%- if raw_cluster_by is not none -%}
  cluster by
  {% if raw_cluster_by is string -%}
    {% set raw_cluster_by = [raw_cluster_by] %}
  {%- endif -%}
  {%- for cluster in raw_cluster_by -%}
    {{ cluster }}
    {%- if not loop.last -%},{%- endif -%}
  {%- endfor -%}

  {% endif %}

{%- endmacro -%}

{% macro bigquery_table_options(persist_docs, temporary, kms_key_name, labels) %}
  {% set opts = {} -%}

  {%- set description = get_relation_comment(persist_docs, model) -%}
  {%- if description is not none -%}
    {%- do opts.update({'description': "'" ~ description ~ "'"}) -%}
  {%- endif -%}
  {%- if temporary and temporary != 'scripting' -%}
    {% do opts.update({'expiration_timestamp': 'TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 hour)'}) %}
  {%- endif -%}
  {%- if kms_key_name -%}
    {%- do opts.update({'kms_key_name': "'" ~ kms_key_name ~ "'"}) -%}
  {%- endif -%}
  {%- if labels -%}
    {%- set label_list = [] -%}
    {%- for label, value in labels.items() -%}
      {%- do label_list.append((label, value)) -%}
    {%- endfor -%}
    {%- do opts.update({'labels': label_list}) -%}
  {%- endif -%}

  {% set options -%}
    OPTIONS({% for opt_key, opt_val in opts.items() %}
      {{ opt_key }}={{ opt_val }}{{ "," if not loop.last }}
    {% endfor %})
  {%- endset %}
  {%- do return(options) -%}
{%- endmacro -%}

{% macro bigquery__create_table_as(temporary, relation, sql) -%}
  {%- set raw_partition_by = config.get('partition_by', none) -%}
  {%- set raw_cluster_by = config.get('cluster_by', none) -%}
  {%- set raw_persist_docs = config.get('persist_docs', {}) -%}
  {%- set raw_kms_key_name = config.get('kms_key_name', none) -%}
  {%- set raw_labels = config.get('labels', []) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  
  {%- set partition_by_dict = parse_partition_by(raw_partition_by) -%}

  {{ sql_header if sql_header is not none }}
  
  {%- if temporary == 'scripting' -%}
  {# "true" temp tables only possible when scripting #}
  
  create or replace temp table {{ relation.identifier }}
  as (
    {{ sql }}
  );
  
  {%- else -%}

  create or replace table {{ relation }}
  {{ partition_by(partition_by_dict) }}
  {{ cluster_by(raw_cluster_by) }}
  {{ bigquery_table_options(
      persist_docs=raw_persist_docs, temporary=temporary, kms_key_name=raw_kms_key_name,
      labels=raw_labels) }}
  as (
    {{ sql }}
  );
  
  {%- endif -%}
  
{%- endmacro -%}


{% macro bigquery__create_view_as(relation, sql) -%}
  {%- set raw_persist_docs = config.get('persist_docs', {}) -%}
  {%- set raw_labels = config.get('labels', []) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create or replace view {{ relation }}
  {{ bigquery_table_options(persist_docs=raw_persist_docs, temporary=false, labels=raw_labels) }}
  as (
    {{ sql }}
  );
{% endmacro %}

{% macro bigquery__create_schema(database_name, schema_name) -%}
  {{ adapter.create_schema(database_name, schema_name) }}
{% endmacro %}

{% macro bigquery__drop_schema(database_name, schema_name) -%}
  {{ adapter.drop_schema(database_name, schema_name) }}
{% endmacro %}

{% macro bigquery__drop_relation(relation) -%}
  {% call statement('drop_relation') -%}
    drop {{ relation.type }} if exists {{ relation }}
  {%- endcall %}
{% endmacro %}

{% macro bigquery__get_columns_in_relation(relation) -%}
  {{ return(adapter.get_columns_in_relation(relation)) }}
{% endmacro %}


{% macro bigquery__list_relations_without_caching(information_schema, schema) -%}
  {{ return(adapter.list_relations_without_caching(information_schema, schema)) }}
{%- endmacro %}


{% macro bigquery__current_timestamp() -%}
  CURRENT_TIMESTAMP()
{%- endmacro %}


{% macro bigquery__snapshot_string_as_time(timestamp) -%}
    {%- set result = 'TIMESTAMP("' ~ timestamp ~ '")' -%}
    {{ return(result) }}
{%- endmacro %}


{% macro bigquery__list_schemas(database) -%}
  {{ return(adapter.list_schemas()) }}
{% endmacro %}


{% macro bigquery__check_schema_exists(information_schema, schema) %}
  {{ return(adapter.check_schema_exists(information_schema.database, schema)) }}
{% endmacro %}
