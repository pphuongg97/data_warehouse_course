WITH stg_fact_sales_order__source AS(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__orders`
)

, stg_fact_sales_order__rename_column AS(
  SELECT
  order_id AS sales_order_key
  , customer_id AS customer_key
  , picked_by_person_id AS picked_by_person_key
  , order_date AS order_date
  FROM stg_fact_sales_order__source
)

, stg_fact_sales_order__cast_type AS(
  SELECT
  CAST(sales_order_key AS INT) AS sales_order_key
  , CAST(customer_key AS INT) AS customer_key
  , CAST(picked_by_person_key AS INT) AS picked_by_person_key
  , CAST(order_date AS DATE) AS order_date
FROM stg_fact_sales_order__rename_column
)

SELECT
sales_order_key
, fact_line.customer_key
, COALESCE(fact_line.picked_by_person_key,0) AS picked_by_person_key
, fact_header.full_name AS full_name
, fact_line.order_date
FROM stg_fact_sales_order__cast_type AS fact_line
LEFT JOIN {{ ref("dim_person")}} AS fact_header
ON fact_line.picked_by_person_key = fact_header.person_key