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

, stg_dim_color__add_undefined_record AS(
  SELECT
    color_key
    , color_name
  FROM stg_dim_color__cast_type

  UNION ALL
  SELECT
    0 AS color_key
    , 'Undefined' AS color_name

  UNION ALL
  SELECT
    -1 AS color_key
    , 'Unvalid' AS color_name
)

SELECT
color_key
, color_name
FROM stg_dim_color__add_undefined_record
