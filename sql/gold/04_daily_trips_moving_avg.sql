-- Daily trips with a 7-day moving average (the window function from the weather project)
SELECT pickup_date,
       trips,
       ROUND(AVG(trips) OVER (
           ORDER BY pickup_date
           ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
       ), 0) AS trips_7d_avg
FROM agg_daily
ORDER BY pickup_date DESC
LIMIT 14