WITH fact_sales_order_line_source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__order_lines`
)

, fact_sales_order_line_rename_column AS(
SELECT 
  order_line_id as sales_order_line_key
  ,stock_item_id as product_key
  ,quantity as quantity
  ,unit_price as unit_price  
FROM fact_sales_order_line_source
)


