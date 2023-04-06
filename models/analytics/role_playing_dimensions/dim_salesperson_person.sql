SELECT
person_key AS salesperson_person_key
, full_name AS salesperson_person_full_name
, preferred_name AS salesperson_person_preferred_name
, phone_number AS salesperson_person_phone_number
, fax_number AS salesperson_person_fax_number
FROM {{ ref("dim_person") }}
WHERE
  is_salesperson = 'Salesperson'
  OR person_key IN (0, -1)

