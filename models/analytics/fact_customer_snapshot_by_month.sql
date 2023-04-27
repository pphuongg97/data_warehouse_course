WITH fact_customer_snapshot_by_month__from_so_line AS(
SELECT
DISTINCT(DATE_TRUNC(order_date, MONTH)) AS year_month
, customer_key
, SUM(gross_amount) AS sales_amount
, COUNT(DISTINCT(sales_order_key)) AS sales_orders
, SUM(gross_amount) / COUNT(DISTINCT(sales_order_key)) AS avg_spend_per_order
FROM {{ ref("fact_sales_order_line") }}
group by DATE_TRUNC(order_date, MONTH), customer_key
)

, fact_customer_snapshot_by_month__lifetime AS(
  SELECT
  *
  , SUM(sales_amount) OVER(PARTITION BY customer_key ORDER BY year_month) AS lifetime_sales_amount
  FROM fact_customer_snapshot_by_month__from_so_line
)

, fact_customer_snapshot_by_month__calculate_percentile AS(
  SELECT
  *
  , PERCENT_RANK() OVER (ORDER BY sales_amount) AS sales_amount_percentile
  , PERCENT_RANK() OVER (ORDER BY lifetime_sales_amount) AS lifetime_sales_amount_percentile
  , PERCENT_RANK() OVER (ORDER BY avg_spend_per_order) AS lifetime_avg_spend_per_order_percentile
  FROM fact_customer_snapshot_by_month__lifetime
)

, fact_customer_snapshot_by_month__segment AS (
  SELECT
  *  
  , CASE
    WHEN sales_amount_percentile BETWEEN 0.8 AND 1 THEN 'High'
    WHEN sales_amount_percentile BETWEEN 0.5 AND 0.8 THEN 'Medium'
    WHEN sales_amount_percentile BETWEEN 0 AND 0.5 THEN 'Low'
    ELSE 'Invalid' END
    AS montary_segment  

  , CASE
    WHEN lifetime_sales_amount_percentile BETWEEN 0.8 AND 1 THEN 'High'
    WHEN lifetime_sales_amount_percentile BETWEEN 0.5 AND 0.8 THEN 'Medium'
    WHEN lifetime_sales_amount_percentile BETWEEN 0 AND 0.5 THEN 'Low'
    ELSE 'Invalid' END
    AS lifetime_montary_segment  

  , CASE
    WHEN lifetime_avg_spend_per_order_percentile BETWEEN 0.8 AND 1 THEN 'High'
    WHEN lifetime_avg_spend_per_order_percentile BETWEEN 0.5 AND 0.8 THEN 'Medium'
    WHEN lifetime_avg_spend_per_order_percentile BETWEEN 0 AND 0.5 THEN 'Low'
    ELSE 'Invalid' END
    AS lifetime_avg_spend_per_order_segment

  FROM fact_customer_snapshot_by_month__calculate_percentile   
)

SELECT
  year_month
  , customer_key
  , sales_amount
  , sales_orders
  , avg_spend_per_order
  , lifetime_sales_amount
  , sales_amount_percentile
  , lifetime_sales_amount_percentile
  , lifetime_avg_spend_per_order_percentile
  , montary_segment
  , lifetime_montary_segment
  , lifetime_avg_spend_per_order_segment

FROM fact_customer_snapshot_by_month__segment
