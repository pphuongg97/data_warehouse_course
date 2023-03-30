WITH dim_person__source AS(
SELECT 
  *
FROM `vit-lam-data.wide_world_importers.application__people`
)

, dim_person__rename_column AS(
  SELECT
  person_id AS person_key
  , full_name AS full_name
  , preferred_name AS preferred_name
  , is_system_user AS is_system_user_boolean
  , is_salesperson AS is_salesperson_boolean
  , is_employee AS is_employee_boolean
  , phone_number AS phone_number
  , fax_number AS fax_number
  FROM dim_person__source
)

, dim_person__cast_type AS(
  SELECT
  CAST(person_key AS INT) AS person_key
  , CAST(full_name AS STRING) AS full_name
  , CAST(preferred_name AS STRING) AS preferred_name
  , CAST(is_system_user_boolean AS BOOLEAN) AS is_system_user_boolean
  , CAST(is_salesperson_boolean AS BOOLEAN) AS is_salesperson_boolean
  , CAST(is_employee_boolean AS BOOLEAN) AS is_employee_boolean
  , CAST(phone_number AS STRING) AS phone_number
  , CAST(fax_number AS STRING) AS fax_number
  FROM dim_person__rename_column
)

, dim_person__convert_boolean AS(
  SELECT
  *
  , CASE
      WHEN is_system_user_boolean IS TRUE THEN 'System User'
      WHEN is_system_user_boolean IS FALSE THEN 'Not System User'
      WHEN is_system_user_boolean IS NULL THEN 'Undefined'
      ELSE 'Unvalid' END
      AS is_system_user
  , CASE
      WHEN is_salesperson_boolean IS TRUE THEN 'Salesperson'
      WHEN is_salesperson_boolean IS FALSE THEN 'Not Salesperson'
      WHEN is_salesperson_boolean IS NULL THEN 'Undefined'
      ELSE 'Unvalid' END
      AS is_salesperson
  , CASE
       WHEN is_employee_boolean IS TRUE THEN 'Employee'
      WHEN is_employee_boolean IS FALSE THEN 'Not Employee'
      WHEN is_employee_boolean IS NULL THEN 'Undefined'
      ELSE 'Unvalid' END
      AS is_employee
FROM dim_person__cast_type
)

, dim_person__add_undefined_record AS(
  SELECT
  person_key
  , full_name
  , preferred_name
  , is_system_user
  , is_salesperson
  , is_employee
  , phone_number
  , fax_number
FROM dim_person__convert_boolean

  UNION ALL
  SELECT
  0 AS person_key
  , 'Undefined' AS full_name
  , 'Undefined' AS preferred_name
  , 'Undefined' AS is_system_user
  , 'Undefined' AS is_salesperson
  , 'Undefined' AS is_employee
  , 'Undefined' AS phone_number
  , 'Undefined' AS fax_number
  UNION ALL
  SELECT
  -1 AS person_key
  , 'Error' AS full_name
  , 'Error' AS preferred_name
  , 'Error' AS is_system_user
  , 'Error' AS is_salesperson
  , 'Error' AS is_employee
  , 'Error' AS phone_number
  , 'Error' AS fax_number
)

SELECT
person_key
, full_name
, preferred_name
, is_system_user
, is_salesperson
, is_employee
, phone_number
, fax_number
FROM dim_person__add_undefined_record


