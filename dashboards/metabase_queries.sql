-- DASHBOARD 1: Current weather KPI cards (latest snapshot)
SELECT
    city,
    maxIf(value, metric = 'temperature_c') AS temperature_c,
    maxIf(value, metric = 'humidity_pct') AS humidity_pct,
    maxIf(value, metric = 'wind_speed_kmh') AS wind_speed_kmh
FROM current_weather_metrics
WHERE event_time >= now() - INTERVAL 1 DAY
GROUP BY city
ORDER BY city;

-- DASHBOARD 2: Daily average temperature trend (weather history)
SELECT
    toDate(event_time) AS day,
    city,
    avg(value) AS avg_temperature_c
FROM hourly_weather_metrics
WHERE metric = 'temperature_c'
GROUP BY day, city
ORDER BY day DESC, city;

-- DASHBOARD 3: Daily precipitation by city
SELECT
    toDate(event_time) AS day,
    city,
    sum(value) AS total_precipitation_mm
FROM hourly_weather_metrics
WHERE metric = 'precipitation_mm'
GROUP BY day, city
ORDER BY day DESC, city;

-- DASHBOARD 4: Air quality trend (PM2.5)
SELECT
    event_time,
    city,
    value AS pm2_5
FROM air_quality_metrics
WHERE metric = 'pm2_5'
ORDER BY event_time DESC
LIMIT 3000;

-- DASHBOARD 5: Air quality heat map source (city x day)
SELECT
    toDate(event_time) AS day,
    city,
    avgIf(value, metric = 'pm2_5') AS avg_pm2_5,
    avgIf(value, metric = 'ozone') AS avg_ozone
FROM air_quality_metrics
GROUP BY day, city
ORDER BY day DESC, city;
