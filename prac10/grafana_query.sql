SELECT
    toStartOfMinute(event_time) AS time,
    city,
    avg(pm25) AS pm25
FROM default.air_quality
WHERE $__timeFilter(event_time)
GROUP BY time, city
ORDER BY time
