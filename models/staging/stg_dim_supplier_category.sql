WITH stg_dim_supplier_category__source AS(
    SELECT
    *
    FROM `vit-lam-data.wide_world_importers.purchasing__supplier_categories`
)

, stg_dim_supplier_category__rename_column AS(
  SELECT
  supplier_category_id AS supplier_category_key
  , supplier_category_name AS supplier_category_name
  FROM stg_dim_supplier_category__source
)

,  stg_dim_supplier_category__cast_type AS(
  SELECT
  CAST(supplier_category_key AS INT) AS supplier_category_key
  , CAST(supplier_category_name AS STRING) AS supplier_category_name
  FROM stg_dim_supplier_category__rename_column
)

SELECT
supplier_category_key
, supplier_category_name
FROM stg_dim_supplier_category__cast_type
