-- Which pickup zones generate the most trips and revenue?
SELECT z.borough,
       z.zone,
       COUNT(*) AS trips,
       ROUND(SUM(f.total_amount), 2) AS revenue
FROM fact_trips f
JOIN dim_zone z ON z.zone_key = f.pu_zone_key
GROUP BY z.borough, z.zone
ORDER BY trips DESC
LIMIT 10