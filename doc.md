# Data Engineering Project

End-to-end multi-pipeline data platform using Airflow, Kafka (Redpanda), ClickHouse, Delta Lake, and Metabase.

## 1) What this project includes

- **3 production-style pipelines**
  - `current_weather_to_kafka_clickhouse_delta`
  - `hourly_weather_to_kafka_clickhouse_delta`
  - `air_quality_to_kafka_clickhouse_delta`
- **Data domains**
  - Current weather snapshot
  - Historical + near-term hourly weather
  - Historical + near-term hourly air quality
- **Storage targets**
  - ClickHouse analytical tables
  - Delta Lake append-only datasets
- **Visualization**
  - Metabase with ClickHouse driver bundled in the container image

## 2) Architecture

`Open-Meteo APIs -> Airflow DAGs -> Kafka topics -> ClickHouse + Delta Lake -> Metabase`

## 3) Repository structure

- `docker-compose.yml`: local stack orchestration
- `airflow/Dockerfile`: Airflow image with required Python libraries
- `metabase/Dockerfile`: Metabase image with bundled ClickHouse driver
- `dags/weather_pipeline.py`: all pipeline logic and DAG definitions
- `dashboards/metabase_queries.sql`: chart queries for Metabase

## 4) Prerequisites

- Docker Desktop (or Docker Engine + Compose plugin)
- Available ports:
  - `8080` (Airflow)
  - `3000` (Metabase)
  - `8123` and `9000` (ClickHouse)
  - `19092` and `9644` (Redpanda)

## 5) Run from scratch

1. Build and start all services:

```bash
docker compose up --build -d
```

2. Confirm containers are healthy/running:

```bash
docker compose ps
```

3. Open Airflow:
- URL: `http://localhost:8080`
- User: `admin`
- Password: `admin`

4. Open Metabase:
- URL: `http://localhost:3000`

## 6) First-run workflow (recommended)

Trigger all DAGs once to materialize data immediately:

```bash
docker compose exec -T airflow airflow dags trigger current_weather_to_kafka_clickhouse_delta
docker compose exec -T airflow airflow dags trigger hourly_weather_to_kafka_clickhouse_delta
docker compose exec -T airflow airflow dags trigger air_quality_to_kafka_clickhouse_delta
```

Check run status:

```bash
docker compose exec -T airflow airflow dags list-runs -d current_weather_to_kafka_clickhouse_delta --no-backfill | head -n 5
docker compose exec -T airflow airflow dags list-runs -d hourly_weather_to_kafka_clickhouse_delta --no-backfill | head -n 5
docker compose exec -T airflow airflow dags list-runs -d air_quality_to_kafka_clickhouse_delta --no-backfill | head -n 5
```

## 7) Data outputs

### ClickHouse tables

- `current_weather_metrics`
- `hourly_weather_metrics`
- `air_quality_metrics`

Quick counts:

```bash
docker compose exec -T clickhouse clickhouse-client \
  --user airflow --password airflow \
  --multiquery --query "
SELECT 'current_weather_metrics', count(*) FROM current_weather_metrics;
SELECT 'hourly_weather_metrics', count(*) FROM hourly_weather_metrics;
SELECT 'air_quality_metrics', count(*) FROM air_quality_metrics;"
```

### Delta Lake paths

- `/opt/airflow/lakehouse/weather_metrics_current`
- `/opt/airflow/lakehouse/weather_metrics_hourly`
- `/opt/airflow/lakehouse/weather_metrics_air_quality`

On host (mounted directory):
- `./lakehouse/weather_metrics_current`
- `./lakehouse/weather_metrics_hourly`
- `./lakehouse/weather_metrics_air_quality`

## 8) Metabase setup and checking data

1. In Metabase, go to `Admin -> Databases -> Add database`.
2. Choose `ClickHouse`.
3. Use:
   - Host: `clickhouse`
   - Port: `8123`
   - Database: `default`
   - Username: `airflow`
   - Password: `airflow`
4. Save and run a test query:

```sql
SELECT count(*) FROM hourly_weather_metrics;
```

Starter dashboard queries are provided in:
- `dashboards/metabase_queries.sql`

## 9) Operations guide

Start:

```bash
docker compose up -d
```

Stop:

```bash
docker compose down
```

Stop and remove volumes (full local reset):

```bash
docker compose down -v
```

Rebuild only one service:

```bash
docker compose build airflow
docker compose build metabase
```

View service logs:

```bash
docker compose logs -f airflow
docker compose logs -f clickhouse
docker compose logs -f metabase
```

## 11) Scaling guide

### A) Increase data volume quickly

In `dags/weather_pipeline.py`:
- Add more cities in `CITIES`.
- Increase `past_days` in `_build_api_params()` for hourly pipelines.
- Increase frequency by changing DAG schedules (for example from `@daily` to `@hourly`).

### B) Increase throughput

- **Kafka/Redpanda**
  - Increase topic partitions and replication in larger environments.
- **Airflow**
  - Current config uses `SequentialExecutor` (simple local mode).
  - For higher concurrency, migrate to `LocalExecutor` or `CeleryExecutor`.
  - Increase worker/scheduler resources and DAG/task parallelism settings.
- **ClickHouse**
  - Keep partitioning by month (`toYYYYMM(event_time)`), and order by `city,event_time,metric`.
  - Tune memory and merge settings based on host capacity.

### C) Storage and retention

- Add retention policies by partition/date for ClickHouse tables.
- Compact/archive older Delta partitions for long-term storage management.

## 12) How to add a new pipeline

All pipeline onboarding is centralized in `dags/weather_pipeline.py`.

1. Add pipeline config in `PIPELINES`:
- `dag_id`
- `topic`
- `table`
- `delta_path`
- `api_url`
- `parser`
- `domain`

2. Implement parser function:
- Input: `city`, `lat`, `lon`, API payload
- Output: list of normalized metric records with:
  - `event_time`, `city`, `latitude`, `longitude`, `domain`, `metric`, `value`, `unit`, `source`, `ingested_at`

3. Add API params branch in `_build_api_params()`.

4. Register DAG by adding another `_build_dag(...)` call at file bottom.

5. Validate and run:

```bash
python3 -m compileall dags
docker compose exec -T airflow airflow dags list | rg "<new_dag_id>"
docker compose exec -T airflow airflow dags trigger <new_dag_id>
```

6. Add Metabase chart SQL in `dashboards/metabase_queries.sql`.

## 13) Troubleshooting

### Metabase does not show ClickHouse option

- Rebuild and restart Metabase image:

```bash
docker compose build metabase
docker compose up -d --force-recreate metabase
```

### Port `3000` or `8080` already in use

- Stop conflicting local process/container, then rerun:

```bash
docker compose up -d
```

### DAG exists but no data arrives

- Check Airflow task logs for producer/consumer tasks.
- Check ClickHouse credentials in `docker-compose.yml`.
- Ensure all services are running via `docker compose ps`.

### Verify all DAG IDs detected by Airflow

```bash
docker compose exec -T airflow airflow dags list | rg "current_weather|hourly_weather|air_quality"
```
