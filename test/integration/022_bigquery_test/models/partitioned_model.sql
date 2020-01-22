
{{
	config(
		materialized = "table",
		partition_by={'field': 'updated_at'},
	)
}}

select * from {{ ref('view_model') }}
