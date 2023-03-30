WITH stg_dim_color__source AS(
    SELECT
    *
    FROM `vit-lam-data.wide_world_importers.warehouse__colors`
)

, stg_dim_color__rename_column AS(
  SELECT
  color_id AS color_key
  , color_name AS color_name
  FROM stg_dim_color__source
)

, stg_dim_color__cast_type AS(
  SELECT
  CAST(color_key AS INT) AS color_key
  , CAST(color_name AS STRING) AS color_name
  FROM stg_dim_color__rename_column
)

SELECT
color_key
, color_name
FROM stg_dim_color__cast_type
