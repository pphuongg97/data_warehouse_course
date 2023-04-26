WITH fact_salesperson_target__from_target AS(
  SELECT
    year_month
  , salesperson_person_key
  , target_gross_amount
  FROM {{ ref('stg_fact_salesperson_target') }}
)

, fact_salesperson_target__from_sales AS(
  SELECT
    CAST (DATE_TRUNC(order_date, MONTH) AS DATE) AS year_month
    , salesperson_person_key
    , SUM(gross_amount) AS gross_amount
  FROM {{ ref ("fact_sales_order_line") }}
  GROUP BY
    1,2
)

, fact_salesperson_target__join AS(
SELECT
  year_month
  , salesperson_person_key
  , COALESCE (fact_target.target_gross_amount, 0) AS target_gross_amount
  , COALESCE (fact_actual.gross_amount, 0) AS gross_amount
FROM fact_salesperson_target__from_target AS fact_target
FULL OUTER JOIN fact_salesperson_target__from_sales AS fact_actual
 USING (year_month, salesperson_person_key)
)

, fact_salesperson_target__calculate_achieve AS(
SELECT
  *
  , gross_amount / IFNULL(target_gross_amount,0) AS achievement_percentage
FROM fact_salesperson_target__join
)

, fact_salesperson_target__define_achievement AS(
SELECT
  *
  , CASE
      WHEN achievement_percentage < 0.95 THEN 'Not Achieved'
      WHEN achievement_percentage >= 0.95 THEN 'Achieved'
      WHEN achievement_percentage = 0 THEN 'No Target'
      ELSE 'Invalid' END
AS is_achieved
FROM fact_salesperson_target__calculate_achieve
)

SELECT
  year_month
  , salesperson_person_key
  , target_gross_amount
  , gross_amount
  , achievement_percentage
  , is_achieved
FROM fact_salesperson_target__define_achievement
