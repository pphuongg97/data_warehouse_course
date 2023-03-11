WITH stg_fact_sales_order__source AS(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__orders`
)

, stg_fact_sales_order__rename_column AS(
  SELECT
  order_id AS sales_order_key
  , customer_id AS customer_key
  FROM stg_fact_sales_order__source
)

, stg_fact_sales_order__cast_type AS(
  SELECT
  CAST(sales_order_key AS INT) AS sales_order_key
  , CAST(customer_key AS INT) AS customer_key
FROM stg_fact_sales_order__rename_column
)

SELECT
sales_order_key
, customer_key
FROM stg_fact_sales_order__cast_type