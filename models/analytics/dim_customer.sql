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

