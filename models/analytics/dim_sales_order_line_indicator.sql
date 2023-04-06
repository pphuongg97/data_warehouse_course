WITH dim_sales_order_line_indicator AS(
SELECT
  TRUE AS is_under_supply_backed_order_boolean
  , 'Under Supply Backed Order' AS is_under_supply_backed_order

UNION ALL
SELECT
  FALSE AS is_under_supply_backed_order_boolean
  , 'Not Under Supply Backed Order' AS is_under_supply_backed_order
)

SELECT
 FARM_FINGERPRINT( CONCAT(dim_sales_order_line_indicator.is_under_supply_backed_order_boolean, "," , stg_dim_package_type.package_type_key) ) AS sales_order_line_indicator_key
 , dim_sales_order_line_indicator.is_under_supply_backed_order_boolean
 , dim_sales_order_line_indicator.is_under_supply_backed_order
 , stg_dim_package_type.package_type_key
 , stg_dim_package_type.package_type_name 
FROM dim_sales_order_line_indicator

CROSS JOIN {{ ref("stg_dim_package_type") }} AS stg_dim_package_type

