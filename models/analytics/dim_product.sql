WITH dim_product__source as(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`
)

, dim_product__rename_column as(
  SELECT 
  stock_item_id as product_key
  , stock_item_name as product_name
  , is_chiller_stock as is_chiller_stock
  , supplier_id as supplier_key
  , brand as brand_name
    
  FROM dim_product__source
)

, dim_product__cast_type as(
  SELECT
  CAST (product_key as int) AS product_key
  , CAST (product_name as string) AS product_name
  , CAST (is_chiller_stock as boolean) AS is_chiller_stock
  , CAST (supplier_key as int) AS supplier_key
  , CAST (brand_name as string) AS brand_name
  FROM dim_product__rename_column
)

,dim_product__convert_boolean AS(
  SELECT
  product_key
  , product_name
  , CASE
      WHEN is_chiller_stock is TRUE THEN 'Chiller Stock'
      WHEN is_chiller_stock is FALSE THEN 'Not Chiller Stock'
      WHEN is_chiller_stock is NULL THEN 'Undefined'
      ELSE 'Unvalid' END 
    AS is_chiller_stock  
  , supplier_key
  , brand_name
  FROM dim_product__rename_column
)

SELECT
  dim_product.product_key
  , dim_product.product_name
  , dim_product.is_chiller_stock
  , dim_product.supplier_key
  , COALESCE(dim_supplier.supplier_name,'Unvalied') AS supplier_name
  , COALESCE(dim_product.brand_name,'Undefined') AS brand_name  
  FROM dim_product__convert_boolean AS dim_product
  LEFT JOIN {{ ref('dim_supplier') }} AS dim_supplier
  ON dim_product.supplier_key = dim_supplier.supplier_key