-- Daily average temperature by city
SELECT
    toDate(event_time) AS day,
    city,
    avg(temperature_c) AS avg_temp_c
FROM weather_metrics
GROUP BY day, city
ORDER BY day DESC, city;

-- Latest humidity snapshot
SELECT
    city,
    argMax(humidity_pct, event_time) AS latest_humidity_pct
FROM weather_metrics
GROUP BY city
ORDER BY city;

-- Wind speed trend
SELECT
    event_time,
    city,
    wind_speed_kmh
FROM weather_metrics
ORDER BY event_time DESC
LIMIT 200;
