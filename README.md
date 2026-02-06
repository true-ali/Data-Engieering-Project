# Minimal Data Engineering Project

This repository contains a minimal end-to-end implementation of the course project requirements:

- **Data source:** Open-Meteo weather API (3 cities)
- **Orchestration:** Apache Airflow (daily schedule from fixed start date)
- **Streaming layer:** Apache Kafka (Redpanda)
- **Analytical storage:** ClickHouse
- **Lakehouse storage:** Delta Lake
- **Dashboard tool:** Metabase (connected to ClickHouse)

## Architecture

`Open-Meteo API -> Airflow DAG -> Kafka topic -> ClickHouse + Delta Lake -> Metabase`

## Project Structure

- `docker-compose.yml`: full local stack
- `airflow/Dockerfile`: Airflow image with required Python dependencies
- `dags/weather_pipeline.py`: daily ingestion and loading DAG
- `dashboards/metabase_queries.sql`: starter queries for dashboard charts

## Run

1. Start services:

```bash
docker compose up --build
```

2. Open Airflow:
- URL: `http://localhost:8080`
- User: `admin`
- Password: `admin`

3. Open Metabase:
- URL: `http://localhost:3000`

4. In Metabase, add ClickHouse as a data source:
- Host: `clickhouse`
- Port: `8123`
- Database: `default`
- User: `airflow`
- Password: `airflow`

5. Trigger or wait for DAG `weather_to_kafka_clickhouse_delta`.

## What the DAG Does

1. Fetches current weather from Open-Meteo for Tehran, Mashhad, and Shiraz.
2. Publishes each city record to Kafka topic `weather_raw`.
3. Consumes a micro-batch from Kafka.
4. Writes records to:
   - ClickHouse table `weather_metrics`
   - Delta table at `/opt/airflow/lakehouse/weather_metrics`

## Requirement Mapping

- **Phase 1 (Data source):** Open-Meteo API selected.
- **Phase 2 (Pipeline design):** Airflow DAG scheduled daily from `2026-01-01`.
- **Phase 3 (Storage):** Kafka -> ClickHouse and Kafka -> Delta Lake.
- **Phase 4 (Dashboard):** Metabase container provided and connected to ClickHouse.
