
{{
	config(
		materialized = "table",
		partition_by={'field': 'updated_at'},
		cluster_by = ["dupe","id"],
	)
}}

select * from {{ ref('view_model') }}
