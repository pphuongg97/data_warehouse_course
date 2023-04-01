WITH stg_fact_sales_order__source AS(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__orders`
)

, stg_fact_sales_order__rename_column AS(
  SELECT
  order_id AS sales_order_key
  , is_undersupply_backordered AS is_unndersupply_backordered_boolean
  , customer_id AS customer_key
  , salesperson_person_id AS salesperson_person_key
  , picked_by_person_id AS picked_by_person_key
  , contact_person_id AS contact_person_key
  , order_date AS order_date
  , expected_delivery_date AS expected_delivery_date
  , picking_completed_when AS picking_completed_when
  FROM stg_fact_sales_order__source
)

, stg_fact_sales_order__cast_type AS(
  SELECT
  CAST(sales_order_key AS INT) AS sales_order_key
  , CAST(is_unndersupply_backordered_boolean AS BOOLEAN) AS is_unndersupply_backordered_boolean
  , CAST(customer_key AS INT) AS customer_key
  , CAST(salesperson_person_key AS INT) salesperson_person_key
  , CAST(picked_by_person_key AS INT) AS picked_by_person_key
  , CAST(contact_person_key AS INT) AS contact_person_key
  , CAST(order_date AS DATE) AS order_date
  , CAST(expected_delivery_date AS DATE) AS expected_delivery_date
  , CAST(picking_completed_when AS DATE) AS picking_completed_when
FROM stg_fact_sales_order__rename_column
)

, stg_fact_sales_order__convert_boolean AS(
    SELECT
    *
    , CASE
      WHEN is_unndersupply_backordered_boolean IS TRUE THEN 'Under Supply Backed Order'
      WHEN is_unndersupply_backordered_boolean IS FALSE THEN 'Not Under Supply Backed Order'
      WHEN is_unndersupply_backordered_boolean IS NULL THEN 'Undefined'
      ELSE 'Unvalid' END
    AS is_undersupply_backordered
    FROM stg_fact_sales_order__cast_type
)

SELECT
sales_order_key
, fact_line.is_undersupply_backordered
, fact_line.customer_key
, COALESCE(fact_line.salesperson_person_key,0) AS salesperson_person_key
, COALESCE(fact_line.picked_by_person_key,0) AS picked_by_person_key
, COALESCE(fact_line.contact_person_key,0) AS contact_person_key
, fact_line.order_date
, fact_line.expected_delivery_date
, fact_line.picking_completed_when

FROM stg_fact_sales_order__convert_boolean AS fact_line

LEFT JOIN {{ ref("dim_person")}} AS fact_header_salesperson_person
ON fact_line.salesperson_person_key = fact_header_salesperson_person.person_key

LEFT JOIN {{ ref("dim_person")}} AS fact_header_picked_by_person
ON fact_line.picked_by_person_key = fact_header_picked_by_person.person_key

LEFT JOIN {{ ref("dim_person")}} AS fact_header_contact_by_person
ON fact_line.contact_person_key = fact_header_contact_by_person.person_key