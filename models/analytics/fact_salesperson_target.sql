WITH fact_salesperson_target AS(
  SELECT
    year_month
  , salesperson_person_key
  , target_gross_amount
  FROM {{ ref('stg_fact_salesperson_target') }}

, fact_salesperson_target__actual_sales AS(
  SELECT
    CAST (DATE_TRUNC(order_date, MONTH) AS DATE) AS year_month
    , salesperson_person_key
    , SUM(gross_amount) AS gross_amount
  FROM {{ ref ("fact_sales_order_line") }}
  GROUP BY
    1,2
)

