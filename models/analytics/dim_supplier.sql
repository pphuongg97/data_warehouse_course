WITH dim_supplier__source AS(
SELECT 
  *
FROM `vit-lam-data.wide_world_importers.purchasing__suppliers`
)

, dim_supplier__rename_column AS(
SELECT
supplier_id AS supplier_key
, supplier_name AS supplier_name
, phone_number AS phone_number
, fax_number AS fax_number
, supplier_category_id AS supplier_category_key
, primary_contact_person_id AS primary_contact_person_key
, alternate_contact_person_id AS alternate_contact_person_key
, delivery_method_id AS delivery_method_key
, delivery_city_id AS delivery_city_key
FROM dim_supplier__source
)

, dim_supplier__cast_type AS(
  SELECT
  CAST (supplier_key AS INT) AS supplier_key
  , CAST (supplier_name AS STRING) AS supplier_name
  , CAST (phone_number AS STRING) AS phone_number
  , CAST (fax_number AS STRING) AS fax_number
  , CAST (supplier_category_key AS INT) AS supplier_category_key
  , CAST (primary_contact_person_key AS INT) AS primary_contact_person_key
  , CAST (alternate_contact_person_key AS INT) AS alternate_contact_person_key
  , CAST (delivery_method_key AS INT) AS delivery_method_key
  , CAST (delivery_city_key AS INT) AS delivery_city_key
  FROM dim_supplier__rename_column
)


SELECT
dim_supplier.supplier_key
, dim_supplier.supplier_name

, dim_supplier.phone_number
, dim_supplier.fax_number

, dim_supplier.supplier_category_key
, COALESCE (stg_dim_supplier_category.supplier_category_name, 'Undefined') AS supplier_category_name

, dim_supplier.primary_contact_person_key
, COALESCE (dim_primary_contact_person.full_name,'Undefined') AS primary_contact_person_full_name

, dim_supplier.alternate_contact_person_key
, COALESCE (dim_alternate_contact_person.full_name,'Undefined') AS alternate_contact_person_full_name

, dim_supplier.delivery_method_key
, COALESCE (stg_dim_delivery_method.delivery_method_name) AS delivery_method_name

, dim_supplier.delivery_city_key
, COALESCE (dim_city.city_name, 'Undefined') AS delivery_city_name

FROM dim_supplier__cast_type AS dim_supplier

LEFT JOIN {{ ref("stg_dim_supplier_category" ) }} AS stg_dim_supplier_category
ON dim_supplier.supplier_category_key = stg_dim_supplier_category.supplier_category_key

LEFT JOIN {{ ref("dim_person") }} AS dim_primary_contact_person
ON dim_supplier.primary_contact_person_key = dim_primary_contact_person.person_key

LEFT JOIN {{ ref("dim_person") }} AS dim_alternate_contact_person
ON dim_supplier.alternate_contact_person_key = dim_alternate_contact_person.person_key

LEFT JOIN {{ ref("stg_dim_delivery_method") }} AS stg_dim_delivery_method
ON dim_supplier.delivery_method_key = stg_dim_delivery_method.delivery_method_key

LEFT JOIN {{ ref("dim_city") }} AS dim_city
ON dim_supplier.delivery_city_key = dim_city.city_key
