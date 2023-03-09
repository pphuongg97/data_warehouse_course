WITH fact_sales_order_line__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__order_lines`
)

, fact_sales_order_line__rename_column AS(
SELECT 
  order_line_id as sales_order_line_key
  ,order_id as sales_order_key
  , stock_item_id as product_key
  , quantity as quantity
  , unit_price as unit_price  
FROM fact_sales_order_line__source
)

, fact_sales_order_line__cast_type AS(
  SELECT
  CAST(sales_order_line_key  AS INT) AS sales_order_line_key
  , CAST(sales_order_key AS INT) AS sales_order_key
  , CAST(product_key AS INT) AS product_key
  , CAST(quantity AS INT) AS quantity
  , CAST(unit_price AS numeric) AS unit_price
  FROM fact_sales_order_line__rename_column
)

SELECT
sales_order_line_key
, sales_order_key
, product_key
, quantity * unit_price AS gross_amount
FROM fact_sales_order_line__cast_type


