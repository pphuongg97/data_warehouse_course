SELECT
person_key AS picked_by_person_key
, full_name AS picked_by_person_full_name
, preferred_name AS picked_by_person_preferred_name
, is_employee
, phone_number AS picked_by_person_phone_number
, fax_number AS picked_by_person_fax_number
FROM {{ ref("dim_person") }}


