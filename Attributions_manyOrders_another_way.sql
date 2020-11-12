WITH
  WebsiteUserStreaming AS (
  SELECT "11111" AS userid, "2020-01-01" AS time, "blog" AS source, "zero" AS event UNION ALL
  SELECT "11111", "2020-01-02", "organic", "CONVERSION" UNION ALL
  --added here rows with another two orders by user '11111'
  SELECT "11111", "2020-01-03", "organic", "CONVERSION" UNION ALL
  SELECT "11111", "2020-01-04", "blog", "CONVERSION" UNION ALL
  SELECT "22222", "2020-01-01", "ppc", "zero" UNION ALL
  SELECT "22222", "2020-01-02", "blog", "zero" UNION ALL
  SELECT "22222", "2020-01-03", "organic", "zero" UNION ALL
  SELECT "22222", "2020-01-04", "ppc", "zero" UNION ALL
  SELECT "22222", "2020-01-05", "organic", "zero" UNION ALL
  SELECT "22222", "2020-01-06", "direct", "zero" UNION ALL
  SELECT "22222", "2020-01-07", "direct", "CONVERSION" UNION ALL
  SELECT "66666", "2020-01-01", "ppc", "CONVERSION" UNION ALL
  SELECT "66666", "2020-01-02", "blog", "zero" UNION ALL
  SELECT "66666", "2020-01-03", "ppc", "zero" UNION ALL
  --added here a row with second order by user '66666'
  SELECT "66666", "2020-01-04", "blog", "CONVERSION"),
  FormatedStreaming AS (
  SELECT
  *,
  --running total (others call it cumulative sum)
  SUM(event) OVER (PARTITION BY userid ORDER BY date_in_sec DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_orders
  FROM(
  SELECT
    userid,
    UNIX_SECONDS(TIMESTAMP(time)) AS date_in_sec,
    source,
    CASE
      WHEN event = 'zero' THEN 0
      WHEN event = 'CONVERSION' THEN 1
  END
    AS event
  FROM
    WebsiteUserStreaming)),
  orderTable AS (
  SELECT
  *,
  CONCAT(userid, '|', total_orders) AS userOrderPath
  FROM
    FormatedStreaming
  )
    
SELECT
userid,
date_in_sec,
source,
total_orders,
position_based,
position,
FROM
(
SELECT
  userid,
  userOrderPath,
  date_in_sec,
  source,
  total_orders,

  CASE
  --one channel and instant conversion
  WHEN MAX(event) OVER (PARTITION BY userOrderPath) > 0 AND COUNT(userOrderPath) OVER (PARTITION BY userOrderPath) = 1
  THEN '1'
  
  WHEN MAX(event) OVER (PARTITION BY userOrderPath) > 0 AND date_in_sec = FIRST_VALUE(date_in_sec) OVER (PARTITION BY userOrderPath ORDER BY date_in_sec) AND COUNT(userOrderPath) OVER (PARTITION BY userOrderPath) > 2
  THEN '0.4'
  
  WHEN MAX(event) OVER (PARTITION BY userOrderPath) > 0 AND date_in_sec = FIRST_VALUE(date_in_sec) OVER (PARTITION BY userOrderPath ORDER BY date_in_sec) AND COUNT(userOrderPath) OVER (PARTITION BY userOrderPath) = 2 THEN '0.5'
  
  WHEN MAX(event) OVER (PARTITION BY userOrderPath) > 0 AND date_in_sec != FIRST_VALUE(date_in_sec) OVER (PARTITION BY userOrderPath ORDER BY date_in_sec) AND date_in_sec != LAST_VALUE(date_in_sec) OVER (PARTITION BY userOrderPath ORDER BY date_in_sec ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
  AND COUNT(userOrderPath) OVER (PARTITION BY userOrderPath) > 2 THEN SAFE_CAST(ROUND(0.2/(COUNT(userOrderPath) OVER (PARTITION BY userOrderPath)-2), 3) AS STRING)
  
  WHEN MAX(event) OVER (PARTITION BY userOrderPath) > 0 AND date_in_sec = LAST_VALUE(date_in_sec) OVER (PARTITION BY userOrderPath ORDER BY date_in_sec) AND COUNT(userOrderPath) OVER (PARTITION BY userOrderPath) > 2
  THEN '0.4'
  
  WHEN MAX(event) OVER (PARTITION BY userOrderPath) > 0 AND date_in_sec = LAST_VALUE(date_in_sec) OVER (PARTITION BY userOrderPath ORDER BY date_in_sec) AND COUNT(userOrderPath) OVER (PARTITION BY userOrderPath) = 2 THEN '0.5'
  ELSE '0'
  END AS position_based,
  
  ROW_NUMBER() OVER (PARTITION BY userOrderPath ORDER BY date_in_sec) as position
FROM
  orderTable)
