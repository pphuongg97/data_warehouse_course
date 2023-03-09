WITH dim_customer_source AS(
  SELECT * 
FROM `vit-lam-data.wide_world_importers.sales__customers`
)

,dim_customer_rename_column AS(
  SELECT
  customer_id AS customer_key
  ,customer_name AS customer_name
  FROM dim_customer_source
)

,dim_customer_cast_type AS(
  SELECT
  CAST(customer_key AS INT) AS customer_key
  ,CAST(customer_name AS STRING) AS customer_name
  FROM dim_customer_rename_column
)

SELECT
customer_key
,customer_name
FROM dim_customer_cast_type
