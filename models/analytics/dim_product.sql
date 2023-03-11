WITH dim_product__source as(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`
)

, dim_product__rename_column as(
  SELECT 
  stock_item_id as product_key
  , stock_item_name as product_name
  , brand as brand_name
  , supplier_id as supplier_key
  , is_chiller_stock as is_chiller_stock
  FROM dim_product__source
)

, dim_product__cast_type as(
  SELECT
  CAST (product_key as int) AS product_key
  , CAST (product_name as string) AS product_name
  , CAST (brand_name as string) AS brand_name
  , CAST (supplier_key as int) AS supplier_key
  , CAST (is_chiller_stock as boolean) AS is_chiller_stock
  FROM dim_product__rename_column
)

,dim_product__convert_boolean AS(
  SELECT
  product_key
  , product_name
  , brand_name
  , supplier_key
  , CASE
      WHEN is_chiller_stock is TRUE THEN 'Chiller Stock'
      WHEN is_chiller_stock is FALSE THEN 'Not Chiller Stock'
      WHEN iss_chiller_stock is NULL THEN 'Undefined'
      ELSE 'Unvalid' END 
    AS is_chiller_stock  
  FROM dim_product__rename_column
)

SELECT
  dim_product.product_key
  , dim_product.product_name
  , dim_product.brand_name
  , dim_product.supplier_key
  , dim_supplier.supplier_name
  , dim_product.is_chiller_stock
  FROM dim_product__convert_boolean AS dim_product
  LEFT JOIN {{ ref('dim_supplier') }} AS dim_supplier
  ON dim_product.supplier_key = dim_supplier.supplier_key