WITH dim_customer__source AS(
  SELECT * 
FROM `vit-lam-data.wide_world_importers.sales__customers`
)

, dim_customer__rename_column AS(
  SELECT
  customer_id AS customer_key
  , customer_name AS customer_name
  , is_statement_sent AS is_statement_sent_boolean
  , is_on_credit_hold AS is_on_credit_hold_boolean
  , credit_limit AS credit_limit
  , standard_discount_percentage AS standard_discount_percentage
  , payment_days AS payment_days
  , phone_number AS phone_number
  , fax_number AS fax_number
  , account_opened_date AS account_opened_date
  , customer_category_id AS customer_category_key
  , buying_group_id AS buying_group_key
  , primary_contact_person_id AS primary_contact_person_key
  , alternate_contact_person_id AS alternate_contact_person_key
  , delivery_method_id AS delivery_method_key
  , bill_to_customer_id AS bill_to_customer_key
  , delivery_city_id AS delivery_city_key
  , postal_city_id AS postal_city_key 
  FROM dim_customer__source
)

,dim_customer__cast_type AS(
  SELECT
  CAST(customer_key AS INT) AS customer_key
  , CAST(customer_name AS STRING) AS customer_name
  , CAST(is_statement_sent_boolean AS BOOLEAN) AS is_statement_sent_boolean
  , CAST(is_on_credit_hold_boolean AS BOOLEAN) AS is_on_credit_hold_boolean
  , CAST(credit_limit AS NUMERIC) AS credit_limit
  , CAST(standard_discount_percentage AS NUMERIC) AS standard_discount_percentage
  , CAST(payment_days AS INT) AS payment_days
  , CAST(phone_number AS STRING) AS phone_number
  , CAST(fax_number AS STRING) AS fax_number
  , CAST(account_opened_date AS DATE) AS account_opened_date  
  , CAST(customer_category_key AS INT) AS customer_category_key
  , CAST(buying_group_key AS INT) AS buying_group_key
  , CAST(primary_contact_person_key AS INT) AS primary_contact_person_key
  , CAST(alternate_contact_person_key AS INT) AS alternate_contact_person_key
  , CAST(delivery_method_key AS INT) AS delivery_method_key
  , CAST(bill_to_customer_key AS INT) AS bill_to_customer_key
  , CAST(delivery_city_key AS INT) AS delivery_city_key
  , CAST(postal_city_key AS INT) AS postal_city_key
  FROM dim_customer__rename_column
)

, dim_customer__convert_boolean AS(
  SELECT 
  *
,   CASE
    WHEN is_statement_sent_boolean IS TRUE THEN 'Statement Sent'
    WHEN is_statement_sent_boolean IS FALSE THEN 'Not Statement Sent'
    WHEN is_statement_sent_boolean IS NULL THEN 'Undefined'
    ELSE 'Unvalid' END
    AS is_statement_sent
,   CASE
    WHEN is_on_credit_hold_boolean IS TRUE THEN 'On Credit Hold'
    WHEN is_on_credit_hold_boolean IS FALSE THEN 'Not On Credit Hold'
    WHEN is_on_credit_hold_boolean IS NULL THEN 'Undefined'
    ELSE 'Unvalid' END
  AS is_on_credit_hold
FROM dim_customer__cast_type
)

, dim_customer__add_undefined_record AS(
SELECT
customer_key
, customer_name
, is_statement_sent
, is_on_credit_hold
, credit_limit
, standard_discount_percentage
, payment_days
, phone_number
, fax_number
, account_opened_date
, customer_category_key
, buying_group_key
, primary_contact_person_key
, alternate_contact_person_key
, delivery_method_key
, bill_to_customer_key
, delivery_city_key
, postal_city_key
FROM dim_customer__convert_boolean

UNION ALL
SELECT
0 AS customer_key
, 'Undefined' AS customer_name
, 'Undefined' AS is_statement_sent
, 'Undefined' AS is_on_credit_hold
, 0 AS credit_limit
, 0 AS standard_discount_percentage
, 0 AS payment_days
, 'Undefined' AS phone_number
, 'Undefined' AS fax_number
, NULL AS account_opened_date
, 0 AS customer_category_key
, 0 AS buying_group_key
, 0 AS primary_contact_person_key
, 0 AS alternate_contact_person_key
, 0 AS delivery_method_key
, 0 AS bill_to_customer_key
, 0 AS delivery_city_key
, 0 AS postal_city_key

UNION ALL
SELECT
-1 AS customer_key
, 'Unvalid' AS customer_name
, 'Unvalid' AS is_statement_sent
, 'Unvalid' AS is_on_credit_hold
, -1 AS credit_limit
, -1 AS standard_discount_percentage
, -1 AS payment_days
, 'Unvalid' AS phone_number
, 'Unvalid' AS fax_number
, NULL AS account_opened_date
, -1 AS customer_category_key
, -1 AS buying_group_key
, -1 AS primary_contact_person_key
, -1 AS alternate_contact_person_key
, -1 AS delivery_method_key
, -1 AS bill_to_customer_key
, -1 AS delivery_city_key
, -1 AS postal_city_key
)

SELECT
dim_customer.customer_key
, dim_customer.customer_name

, dim_customer.is_statement_sent
, dim_customer.is_on_credit_hold

, dim_customer.credit_limit
, dim_customer.standard_discount_percentage
, dim_customer.payment_days
, dim_customer.phone_number
, dim_customer.fax_number

, dim_customer.account_opened_date

, dim_customer.customer_category_key
, COALESCE(stg_dim_customer_category.customer_category_name,'Unvalid') AS customer_category_name

, dim_customer.buying_group_key
, COALESCE(stg_dim_buying_group.buying_group_name,'Unvalid') AS buying_group_name

, dim_customer.primary_contact_person_key
, COALESCE(dim_primary_contact_person.full_name, 'Unvalid') AS primary_contact_person_full_name

, dim_customer.alternate_contact_person_key
, COALESCE(dim_alternate_contact_person.full_name, 'Unvalid') AS alternate_contact_person_full_name

, dim_customer.delivery_method_key
, COALESCE(stg_dim_delivery_method.delivery_method_name, 'Unvalid') AS delivery_method_name

, dim_customer.bill_to_customer_key
, COALESCE(dim_bill_to_customer.customer_name) AS bill_to_customer_name

, dim_customer.delivery_city_key
, COALESCE(dim_city.city_name,'Unvalid') AS delivery_city_name

, dim_customer.postal_city_key
, COALESCE(dim_postal_city.city_name,'Unvalid') AS postal_city_name

FROM dim_customer__add_undefined_record AS dim_customer

LEFT JOIN dim_customer__convert_boolean AS dim_bill_to_customer
ON dim_customer.bill_to_customer_key = dim_bill_to_customer.customer_key

LEFT JOIN {{ ref("stg_dim_customer_category") }} AS stg_dim_customer_category
ON dim_customer.customer_category_key = stg_dim_customer_category.customer_category_key

LEFT JOIN {{ ref("stg_dim_buying_group")}} AS stg_dim_buying_group
ON dim_customer.buying_group_key = stg_dim_buying_group.buying_group_key

LEFT JOIN {{ ref("dim_person") }} AS dim_primary_contact_person
ON dim_customer.primary_contact_person_key = dim_primary_contact_person.person_key

LEFT JOIN {{ ref("dim_person") }} AS dim_alternate_contact_person
ON dim_customer.alternate_contact_person_key = dim_alternate_contact_person.person_key

LEFT JOIN {{ ref("stg_dim_delivery_method") }} AS stg_dim_delivery_method
ON dim_customer.delivery_method_key = stg_dim_delivery_method.delivery_method_key



LEFT JOIN {{ ref("dim_city") }} AS dim_city
ON dim_customer.delivery_city_key = dim_city.city_key

LEFT JOIN {{ ref("dim_city") }} AS dim_postal_city
ON dim_customer.postal_city_key = dim_postal_city.city_key