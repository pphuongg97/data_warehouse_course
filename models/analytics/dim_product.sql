WITH dim_product__source AS(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`
)

, dim_product__rename_column AS(
  SELECT 
  stock_item_id AS product_key
  , stock_item_name AS product_name
  , barcode AS barcode
  , brand AS brand_name
  , size AS size
  , is_chiller_stock AS is_chiller_stock
  , lead_time_days AS lead_time_days
  , unit_price AS unit_price
  , tax_rate AS tax_rate
  , recommended_retail_price AS recommended_retail_price
  , typical_weight_per_unit AS typical_weight_per_unit
  , supplier_id AS supplier_key
  , color_id AS color_key
  , unit_package_id AS unit_package_type_key
  , outer_package_id AS outer_package_type_key
  , quantity_per_outer AS quantity_per_outer    
  FROM dim_product__source
)

, dim_product__cast_type AS(
  SELECT
  CAST (product_key AS INT) AS product_key
  , CAST (product_name AS STRING) AS product_name
  , CAST (barcode AS STRING) AS barcode
  , CAST (brand_name AS STRING) AS brand_name
  , CAST (size AS STRING) AS size
  , CAST (is_chiller_stock AS BOOLEAN) AS is_chiller_stock
  , CAST (lead_time_days AS INT) AS lead_time_days
  , CAST (unit_price AS NUMERIC) AS unit_price
  , CAST (tax_rate AS NUMERIC) AS tax_rate
  , CAST (recommended_retail_price AS NUMERIC) AS recommended_retail_price
  , CAST (typical_weight_per_unit AS NUMERIC) AS typical_weight_per_unit
  , CAST (supplier_key AS INT) AS supplier_key
  , CAST (color_key AS INT) AS color_key
  , CAST (unit_package_type_key AS INT) AS unit_package_type_key
  , CAST (outer_package_type_key AS INT) AS outer_package_type_key
  , CAST (quantity_per_outer AS NUMERIC) AS quantity_per_outer
  FROM dim_product__rename_column
)

,dim_product__convert_boolean AS(
  SELECT
  *
  , CASE
      WHEN is_chiller_stock is TRUE THEN 'Chiller Stock'
      WHEN is_chiller_stock is FALSE THEN 'Not Chiller Stock'
      WHEN is_chiller_stock is NULL THEN 'Undefined'
      ELSE 'Unvalid' END 
    AS is_chiller_stock
  FROM dim_product__cast_type
)

SELECT
  dim_product.product_key
  , dim_product.product_name

  , dim_product.barcode
  , COALESCE(dim_product.brand_name, 'Undefined') AS brand_name
  , dim_product.size
  , dim_product.is_chiller_stock

  , dim_product.lead_time_days
  , dim_product.unit_price
  , dim_product.tax_rate
  , dim_product.recommended_retail_price
  , dim_product.typical_weight_per_unit

  , dim_product.supplier_key
  , COALESCE(dim_supplier.supplier_name,'Undefined') AS supplier_name
  
  , dim_product.color_key
  , COALESCE(stg_dim_color.color_name,'Undefined') AS color_name

  , dim_product.unit_package_type_key
  , COALESCE(stg_dim_unit_package_type.unit_package_type_name, 'Undefined') AS unit_package_type_name

  , dim_product.outer_package_type_key
  , COALESCE(stg_dim_outer_package_type.outer_package_type_name, 'Undefined') AS outer_package_type_name

  , dim_product.quantity_per_outer

  FROM dim_product__convert_boolean AS dim_product

  LEFT JOIN {{ ref('dim_supplier') }} AS dim_supplier
  ON dim_product.supplier_key = dim_supplier.supplier_key

  LEFT JOIN {{ ref('stg_dim_color') }} AS stg_dim_color
  ON dim_product.color_key = stg_dim_color.color_key

  LEFT JOIN {{ ref("stg_dim_package_type") }} AS stg_dim_unit_package_type
  ON dim_product.unit_package_type_key = stg_dim_unit_package_type.package_type_key

  LEFT JOIN {{ ref("stg_dim_package_type") }} AS stg_dim_outer_package_type
  ON dim_product.outer_package_type_key = stg_dim_outer_package_type.package_type_key  

