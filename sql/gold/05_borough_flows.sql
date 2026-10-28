-- Which borough-to-borough routes are busiest?
SELECT pz.borough AS pickup_borough,
       dz.borough AS dropoff_borough,
       COUNT(*) AS trips,
       ROUND(AVG(f.total_amount), 2) AS avg_total
FROM fact_trips f
JOIN dim_zone pz ON pz.zone_key = f.pu_zone_key
JOIN dim_zone dz ON dz.zone_key = f.do_zone_key
GROUP BY pz.borough, dz.borough
ORDER BY trips DESC
LIMIT 10