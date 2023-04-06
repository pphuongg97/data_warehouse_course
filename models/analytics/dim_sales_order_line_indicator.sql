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
  *
FROM dim_is_under_supply_backed_order

CROSS JOIN stg_dim_package_type AS stg_dim_package_type
ORDER BY 1, 3