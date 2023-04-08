WITH dim_sales_order_line_indicator AS(
SELECT
  TRUE AS is_undersupply_backedorder_boolean
  , 'Under Supply Backed Order' AS is_undersupply_backedorder

UNION ALL
SELECT
  FALSE AS is_undersupply_backedorder_boolean
  , 'Not Under Supply Backed Order' AS is_undersupply_backedorder
)

SELECT
 FARM_FINGERPRINT( CONCAT(dim_sales_order_line_indicator.is_undersupply_backedorder, "," , stg_dim_package_type.package_type_key) ) AS sales_order_line_indicator_key
 , dim_sales_order_line_indicator.is_undersupply_backedorder_boolean
 , dim_sales_order_line_indicator.is_undersupply_backedorder
 , stg_dim_package_type.package_type_key
 , stg_dim_package_type.package_type_name 
FROM dim_sales_order_line_indicator

CROSS JOIN {{ ref("stg_dim_package_type") }} AS stg_dim_package_type

