{{
  config(
    column_schemas={
      "required_value": "INT64 NOT NULL",
      "nullable_value": "STRING",
      "x": "STRUCT<x1 INT64 NOT NULL, x2 STRING> NOT NULL",
      "y": "ARRAY<INT64>"
    }
  )
}}

SELECT
  1 AS required_value,
  "a" AS nullable_value,
  STRUCT(
    1 AS x1,
    "a" AS x2
  ) AS x,
  [1, 2, 3] AS array_value