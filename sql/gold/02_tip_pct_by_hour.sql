-- How does the average credit card tip change through the day?
-- avg_tip_pct: mean of per-trip ratios (skewed up by tiny fares)
-- weighted_tip_pct: total tips / total fares (not skewed by tiny fares)
SELECT pickup_hour,
       COUNT(*) AS card_trips,
       ROUND(AVG(tip_pct) * 100, 1) AS avg_tip_pct,
       ROUND(SUM(tip_amount) / SUM(fare_amount) * 100, 1) AS weighted_tip_pct
FROM fact_trips
WHERE tip_pct IS NOT NULL
GROUP BY pickup_hour
ORDER BY pickup_hour