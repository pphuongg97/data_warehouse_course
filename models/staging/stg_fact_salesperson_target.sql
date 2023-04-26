WITH fact_salesperson_target__source AS(
  SELECT
  *
  FROM `vit-lam-data.wide_world_importers.external__salesperson_target`
)

, fact_salesperson_target__rename_column AS(
  SELECT
  year_month
  , salesperson_person_id AS salesperson_person_key
  , target_revenue AS target_gross_amount
FROM fact_salesperson_target__source
)

, fact_salesperson_target__cast_type AS(
  SELECT
    CAST (DATE_TRUNC(year_month, MONTH) AS DATE) AS year_month
  , CAST (salesperson_person_key AS INT) AS salesperson_person_key
  , CAST (target_gross_amount AS NUMERIC) AS target_gross_amount
FROM fact_salesperson_target__rename_column
)

SELECT
  year_month
  , salesperson_person_key
  , target_gross_amount
FROM fact_salesperson_target__cast_type

