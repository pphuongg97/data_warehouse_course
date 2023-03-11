WITH dim_customer_source AS(
  SELECT * 
FROM `vit-lam-data.wide_world_importers.sales__customers`
)

,dim_customer_rename_column AS(
  SELECT
  customer_id AS customer_key
  , customer_name AS customer_name
  , customer_category_id AS customer_category_key
  , buying_group_id AS buying_group_key 
  FROM dim_customer_source
)

,dim_customer_cast_type AS(
  SELECT
  CAST(customer_key AS INT) AS customer_key
  , CAST(customer_name AS STRING) AS customer_name
  , CAST(customer_category_key AS INT) AS customer_category_key
  , CAST (buying_group_key AS INT) AS buying_group_key
  FROM dim_customer_rename_column
)

SELECT
dim_customer.customer_key
, dim_customer.customer_name
, dim_customer.customer_category_key
, dim_customer.buying_group_key
, stg_dim_customer_category.customer_category_name
, stg_dim_buying_group.buying_group_name
FROM dim_customer_cast_type AS dim_customer
LEFT JOIN {{ ref("stg_dim_customer_category") }} AS stg_dim_customer_category
ON dim_customer.customer_category_key = stg_dim_customer_category.customer_category_key
LEFT JOIN {{ ref("stg_buying_group")}} AS stg_dim_buying_group
ON dim_customer.buying_group_key = stg_dim_buying_group.buying_group_name