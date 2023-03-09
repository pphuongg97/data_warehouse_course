WITH dim_product_source as(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`
)

, dim_product_rename_column as(
  SELECT 
  stock_item_id as product_key,
  stock_item_name as product_name,
  brand as brand_name
  FROM dim_product_source
)

,dim_product_cast_type as(
  SELECT
  CAST (product_key as int) AS product_key,
  CAST (product_name as string) AS product_name,
  CAST (brand_name as string) AS brand_name
  FROM dim_product_rename_column
)

SELECT
  product_key,
  product_name,
  brand_name
  FROM dim_product_cast_type