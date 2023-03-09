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