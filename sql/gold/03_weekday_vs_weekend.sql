-- Do weekends look different from weekdays?
SELECT d.is_weekend,
       COUNT(*) AS trips,
       ROUND(AVG(f.total_amount), 2) AS avg_total,
       ROUND(AVG(f.trip_distance), 2) AS avg_distance_mi
FROM fact_trips f
JOIN dim_date d ON d.date_key = f.pickup_date_key
GROUP BY d.is_weekend
ORDER BY d.is_weekend