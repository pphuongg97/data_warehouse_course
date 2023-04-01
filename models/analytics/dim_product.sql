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
  , is_chiller_stock AS is_chiller_stock_boolean
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
  , CAST (is_chiller_stock_boolean AS BOOLEAN) AS is_chiller_stock_boolean
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

, dim_product__convert_boolean AS(
  SELECT
  *
  , CASE
      WHEN is_chiller_stock_boolean is TRUE THEN 'Chiller Stock'
      WHEN is_chiller_stock_boolean is FALSE THEN 'Not Chiller Stock'
      WHEN is_chiller_stock_boolean is NULL THEN 'Undefined'
      ELSE 'Unvalid' END 
    AS is_chiller_stock
  FROM dim_product__cast_type
)

, dim_product__add_undefined_record AS(
  SELECT
  product_key
  , product_name
  , barcode
  , brand_name
  , size
  , is_chiller_stock
  , lead_time_days
  , unit_price
  , tax_rate
  , recommended_retail_price
  , typical_weight_per_unit
  , supplier_key
  , color_key
  , unit_package_type_key
  , outer_package_type_key
  , quantity_per_outer
  FROM dim_product__convert_boolean

  UNION ALL
  SELECT
   0 AS product_key
  , 'Undefined' product_name
  , 'Undefined' barcode
  , 'Undefined' brand_name
  , 'Undefined' size
  , 'Undefined' is_chiller_stock
  , 0 AS lead_time_days
  , 0 AS unit_price
  , 0 AS tax_rate
  , 0 AS recommended_retail_price
  , 0 AS typical_weight_per_unit
  , 0 AS supplier_key
  , 0 AS color_key
  , 0 AS unit_package_type_key
  , 0 AS outer_package_type_key
  , 0 AS quantity_per_outer

    UNION ALL
  SELECT
   -1 AS product_key
  , 'Unvalid' product_name
  , 'Unvalid' barcode
  , 'Unvalid' brand_name
  , 'Unvalid' size
  , 'Unvalid' is_chiller_stock
  , -1 AS lead_time_days
  , -1 AS unit_price
  , -1 AS tax_rate
  , -1 AS recommended_retail_price
  , -1 AS typical_weight_per_unit
  , -1 AS supplier_key
  , -1 AS color_key
  , -1 AS unit_package_type_key
  , -1 AS outer_package_type_key
  , -1 AS quantity_per_outer
)

SELECT
  dim_product.product_key
  , dim_product.product_name

  , dim_product.barcode
  , COALESCE(dim_product.brand_name, 'Unvalid') AS brand_name
  , dim_product.size
  , dim_product.is_chiller_stock

  , dim_product.lead_time_days
  , dim_product.unit_price
  , dim_product.tax_rate
  , dim_product.recommended_retail_price
  , dim_product.typical_weight_per_unit

  , dim_product.supplier_key
  , COALESCE(dim_supplier.supplier_name,'Unvalid') AS supplier_name
  
  , dim_product.color_key
  , COALESCE(stg_dim_color.color_name,'Unvalid') AS color_name

  , dim_product.unit_package_type_key
  , COALESCE(stg_dim_unit_package_type.package_type_name, 'Unvalid') AS unit_package_type_name

  , dim_product.outer_package_type_key
  , COALESCE(stg_dim_outer_package_type.package_type_name, 'Unvalid') AS outer_package_type_name

  , dim_product.quantity_per_outer

  FROM dim_product__add_undefined_record AS dim_product

  LEFT JOIN {{ ref("dim_supplier") }} AS dim_supplier
  ON dim_product.supplier_key = dim_supplier.supplier_key

  LEFT JOIN {{ ref("stg_dim_color") }} AS stg_dim_color
  ON dim_product.color_key = stg_dim_color.color_key

  LEFT JOIN {{ ref("stg_dim_package_type") }} AS stg_dim_unit_package_type
  ON dim_product.unit_package_type_key = stg_dim_unit_package_type.package_type_key

  LEFT JOIN {{ ref("stg_dim_package_type") }} AS stg_dim_outer_package_type
  ON dim_product.outer_package_type_key = stg_dim_outer_package_type.package_type_key  

