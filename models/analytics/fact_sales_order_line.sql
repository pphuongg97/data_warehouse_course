WITH fact_sales_order_line__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__order_lines`
)

, fact_sales_order_line__rename_column AS(
SELECT 
  order_line_id AS sales_order_line_key
  , order_id AS sales_order_key
  , stock_item_id AS product_key
  , quantity AS quantity
  , unit_price AS unit_price
  , tax_rate AS tax_rate
  , picked_quantity AS picked_quantity
  , package_type_id AS package_type_key
  , picking_completed_when AS line_picking_completed_when
FROM fact_sales_order_line__source
)

, fact_sales_order_line__cast_type AS(
  SELECT
  CAST(sales_order_line_key  AS INT) AS sales_order_line_key
  , CAST(sales_order_key AS INT) AS sales_order_key
  , CAST(product_key AS INT) AS product_key
  , CAST(quantity AS INT) AS quantity
  , CAST(unit_price AS NUMERIC) AS unit_price
  , CAST(tax_rate AS NUMERIC) AS tax_rate
  , CAST(picked_quantity AS INT) AS picked_quantity
  , CAST(package_type_key AS INT) AS package_type_key
  , CAST(line_picking_completed_when AS DATE) AS line_picking_completed_when
  FROM fact_sales_order_line__rename_column
)

, fact_sales_order_line__calculated_measure AS(
  SELECT
  *
  , quantity * unit_price AS gross_amount
  , tax_rate * quantity * unit_price AS tax_amount
  , quantity * unit_price * (1 - tax_rate) AS net_amount
  FROM fact_sales_order_line__cast_type
)

SELECT
sales_order_line_key
, fact_line.sales_order_key
, COALESCE(fact_header.is_undersupply_backordered,'Unvalid') AS is_undersupply_backordered
, fact_line.product_key
, fact_line.quantity
, fact_line.unit_price
, fact_line.gross_amount
, fact_line.tax_rate
, fact_line.tax_amount
, fact_line.net_amount
, fact_line.picked_quantity
, COALESCE(fact_header.customer_key,'Unvalid') AS customer_key
, COALESCE(fact_header.salesperson_person_key,'Unvalid') AS salesperson_person_key
, COALESCE(fact_header.picked_by_person_key,'Unvalid') AS picked_by_person_key
, COALESCE(fact_header.contact_person_key,'Unvalid') AS contact_person_key
, fact_line.package_type_key
, fact_header.order_date
, fact_header.expected_delivery_date
, fact_line.line_picking_completed_when
, fact_header.picking_completed_when AS sales_order_picking_completed_when

FROM fact_sales_order_line__calculated_measure AS fact_line

LEFT JOIN {{ ref('stg_fact_sales_order') }} AS fact_header
ON fact_line.sales_order_key=fact_header.sales_order_key



