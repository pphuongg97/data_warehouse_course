SELECT 
  cast(order_line_id as int) as sales_order_line_key
  ,cast (quantity as int) as quantity
  ,cast(unit_price as numeric) as unit_price
  ,cast(quantity as int)* cast(unit_price as numeric) as gross_amount
  ,cast(stock_item_id as int) as product_key
FROM `vit-lam-data.wide_world_importers.sales__order_lines`