WITH dim_customer_attribute__summarize AS(
  SELECT
    customer_key
    , SUM(gross_amount) AS lifetime_sales_amount
    , COUNT (DISTINCT sales_order_key) AS lifetime_sales_orders
    , SUM( CASE
      WHEN order_date 
      BETWEEN (DATE_TRUNC('2016-05-31',MONTH) - INTERVAL 12 MONTH)
      AND '2016-05-31'
      THEN gross_amount END
    ) AS l12m_sales_amount
    , COUNT (DISTINCT CASE
      WHEN order_date 
      BETWEEN (DATE_TRUNC('2016-05-31',MONTH) - INTERVAL 12 MONTH)
      AND '2016-05-31'
      THEN sales_order_key END
    ) AS l12m_sales_orders
  FROM {{ ref("fact_sales_order_line") }}
  GROUP BY 1
)

, dim_customer_attribute__calculate_avg_spend_per_order AS (
  SELECT
  *
  , lifetime_sales_amount / lifetime_sales_orders AS lifetime_avg_spend_per_order
  , l12m_sales_amount / l12m_sales_orders AS l12m_avg_spend_per_order
  FROM dim_customer_attribute__summarize
)

, dim_customer_attribute__join AS(
  SELECT
    dim_customer_attribute__calculate_avg_spend_per_order.customer_key
    , customer_category_key
    , COALESCE(dim_customer.customer_category_name, 'Unvalid') AS customer_category_name
    , dim_customer.delivery_state_province_key
    , COALESCE(dim_customer.delivery_state_province_name, 'Unvalid') AS delivery_state_province_name
    , dim_customer_attribute__calculate_avg_spend_per_order.lifetime_sales_amount
    , dim_customer_attribute__calculate_avg_spend_per_order.lifetime_sales_orders
    , dim_customer_attribute__calculate_avg_spend_per_order.l12m_sales_amount
    , dim_customer_attribute__calculate_avg_spend_per_order.l12m_sales_orders
    , dim_customer_attribute__calculate_avg_spend_per_order.lifetime_avg_spend_per_order
    , dim_customer_attribute__calculate_avg_spend_per_order.l12m_avg_spend_per_order

  FROM dim_customer_attribute__calculate_avg_spend_per_order

  LEFT JOIN {{ ref("dim_customer") }} AS dim_customer
  ON dim_customer_attribute__calculate_avg_spend_per_order.customer_key = dim_customer.customer_key
)

, dim_customer_attribute__calculate_percentile AS(
  SELECT
    *
    , PERCENT_RANK() OVER (ORDER BY lifetime_sales_amount) AS lifetime_montary_percentile
    , PERCENT_RANK() OVER (ORDER BY l12m_sales_amount) AS l12m_montary_percentile
    , PERCENT_RANK() OVER (ORDER BY lifetime_avg_spend_per_order) AS lifetime_avg_spend_percentile
    , PERCENT_RANK() OVER (ORDER BY l12m_avg_spend_per_order) AS l12m_avg_spend_percentile
  FROM dim_customer_attribute__join
)

, dim_customer_attribute__segment AS(
  SELECT
  *
  , CASE
    WHEN lifetime_montary_percentile BETWEEN 0.8 AND 1 THEN 'High' --Top 20%
    WHEN lifetime_montary_percentile BETWEEN 0.5 AND 0.8 THEN 'Medium'
    WHEN lifetime_montary_percentile BETWEEN 0 AND 0.5 THEN 'Low'--Bottom 50%
    ELSE 'Invalid' END
    AS lifetime_montary_segment

  , CASE
    WHEN l12m_montary_percentile BETWEEN 0.8 AND 1 THEN 'High' --Top 20%
    WHEN l12m_montary_percentile BETWEEN 0.5 AND 0.8 THEN 'Medium'
    WHEN l12m_montary_percentile BETWEEN 0 AND 0.5 THEN 'Low'--Bottom 50%
    ELSE 'Invalid' END
    AS l12m_montary_segment

  , CASE
    WHEN lifetime_avg_spend_percentile BETWEEN 0.8 AND 1 THEN 'High' --Top 20%
    WHEN lifetime_avg_spend_percentile BETWEEN 0.5 AND 0.8 THEN 'Medium'
    WHEN lifetime_avg_spend_percentile BETWEEN 0 AND 0.5 THEN 'Low'--Bottom 50%
    ELSE 'Invalid' END
    AS lifetime_avg_spend_segment

  , CASE
    WHEN l12m_avg_spend_percentile BETWEEN 0.8 AND 1 THEN 'High' --Top 20%
    WHEN l12m_avg_spend_percentile BETWEEN 0.5 AND 0.8 THEN 'Medium'
    WHEN l12m_avg_spend_percentile BETWEEN 0 AND 0.5 THEN 'Low'--Bottom 50%
    ELSE 'Invalid' END
    AS l12m_avg_spend_segment 

  FROM dim_customer_attribute__calculate_percentile
)

SELECT

customer_key
, customer_category_key
, customer_category_name
, delivery_state_province_key
, delivery_state_province_name
, lifetime_sales_amount
, lifetime_sales_orders
, l12m_sales_amount
, l12m_sales_orders
, lifetime_avg_spend_per_order
, l12m_avg_spend_per_order
, lifetime_montary_percentile
, l12m_montary_percentile
, lifetime_avg_spend_percentile
, l12m_avg_spend_percentile
, lifetime_montary_segment
, l12m_montary_segment
, lifetime_avg_spend_segment
, l12m_avg_spend_segment

FROM dim_customer_attribute__segment
