WITH dim_date__generate AS(
SELECT
    *
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2014-01-01', '2050-01-01', INTERVAL 1 DAY)) AS date
)

, dim_date__enrich AS(
SELECT
  FORMAT_DATE('%A', date) AS day_of_week
  , FORMAT_DATE('%a', date) AS day_of_week_short
  , DATE_TRUNC(date,month) AS year_month
  , FORMAT_DATE('%B', date) AS month
  , DATE_TRUNC(date,year) AS year
  ,EXTRACT(YEAR FROM date) AS year_number

FROM dim_date__generate
)

SELECT
  *
  , CASE
    WHEN day_of_week_short IN ('Mon', 'Tue', 'Wed', 'Thu', 'Fri') THEN 'Weekday'
    WHEN day_of_week_short IN ('Sat', 'Sun') THEN 'Weekend'
    ELSE 'Invalid' END
  AS is_weekday_or_weekend
FROM dim_date__enrich
