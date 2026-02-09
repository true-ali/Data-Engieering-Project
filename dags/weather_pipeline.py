from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any

import clickhouse_connect
import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Consumer, Producer
from deltalake import write_deltalake

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "default")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "airflow")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "airflow")

LAKEHOUSE_ROOT = os.getenv("DELTA_TABLE_PATH", "/opt/airflow/lakehouse/weather_metrics")

# Expanding city coverage increases volume and dashboard usefulness.
CITIES = {
    "Tehran": (35.6892, 51.3890),
    "Mashhad": (36.2605, 59.6168),
    "Shiraz": (29.5918, 52.5837),
    "Tabriz": (38.0962, 46.2738),
    "Isfahan": (32.6539, 51.6660),
    "Karaj": (35.8400, 50.9391),
    "Ahvaz": (31.3183, 48.6706),
    "Qom": (34.6416, 50.8756),
    "Kermanshah": (34.3142, 47.0650),
    "Urmia": (37.5527, 45.0760),
    "Rasht": (37.2808, 49.5832),
    "Zahedan": (29.4963, 60.8629),
}

PIPELINES = {
    "current_weather_pipeline": {
        "dag_id": "current_weather_to_kafka_clickhouse_delta",
        "topic": "current_weather_raw",
        "table": "current_weather_metrics",
        "delta_path": f"{LAKEHOUSE_ROOT}_current",
        "domain": "weather",
        "api_url": "https://api.open-meteo.com/v1/forecast",
        "parser": "parse_current_weather",
    },
    "hourly_weather_pipeline": {
        "dag_id": "hourly_weather_to_kafka_clickhouse_delta",
        "topic": "hourly_weather_raw",
        "table": "hourly_weather_metrics",
        "delta_path": f"{LAKEHOUSE_ROOT}_hourly",
        "domain": "weather",
        "api_url": "https://api.open-meteo.com/v1/forecast",
        "parser": "parse_hourly_weather",
    },
    "air_quality_pipeline": {
        "dag_id": "air_quality_to_kafka_clickhouse_delta",
        "topic": "air_quality_raw",
        "table": "air_quality_metrics",
        "delta_path": f"{LAKEHOUSE_ROOT}_air_quality",
        "domain": "air_quality",
        "api_url": "https://air-quality-api.open-meteo.com/v1/air-quality",
        "parser": "parse_hourly_air_quality",
    },
}


def _fetch_json(url: str, params: dict[str, Any]) -> dict[str, Any]:
    response = requests.get(url, params=params, timeout=45)
    response.raise_for_status()
    return response.json()


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def parse_current_weather(city: str, latitude: float, longitude: float, payload: dict[str, Any]) -> list[dict[str, Any]]:
    current = payload.get("current", {})
    units = payload.get("current_units", {})
    event_time = current.get("time")
    if not event_time:
        return []

    records: list[dict[str, Any]] = []
    metrics = {
        "temperature_2m": "temperature_c",
        "relative_humidity_2m": "humidity_pct",
        "wind_speed_10m": "wind_speed_kmh",
        "precipitation": "precipitation_mm",
    }
    now_iso = datetime.now(timezone.utc).isoformat()

    for api_metric, metric_name in metrics.items():
        metric_value = _safe_float(current.get(api_metric))
        if metric_value is None:
            continue
        records.append(
            {
                "event_time": event_time,
                "city": city,
                "latitude": latitude,
                "longitude": longitude,
                "domain": "weather",
                "metric": metric_name,
                "value": metric_value,
                "unit": units.get(api_metric, ""),
                "source": "open-meteo",
                "ingested_at": now_iso,
            }
        )

    return records


def parse_hourly_weather(city: str, latitude: float, longitude: float, payload: dict[str, Any]) -> list[dict[str, Any]]:
    hourly = payload.get("hourly", {})
    units = payload.get("hourly_units", {})
    times = hourly.get("time", [])
    if not times:
        return []

    metric_map = {
        "temperature_2m": "temperature_c",
        "relative_humidity_2m": "humidity_pct",
        "wind_speed_10m": "wind_speed_kmh",
        "precipitation": "precipitation_mm",
        "surface_pressure": "surface_pressure_hpa",
    }
    now_iso = datetime.now(timezone.utc).isoformat()
    records: list[dict[str, Any]] = []

    for idx, event_time in enumerate(times):
        for api_metric, metric_name in metric_map.items():
            series = hourly.get(api_metric, [])
            if idx >= len(series):
                continue
            metric_value = _safe_float(series[idx])
            if metric_value is None:
                continue
            records.append(
                {
                    "event_time": event_time,
                    "city": city,
                    "latitude": latitude,
                    "longitude": longitude,
                    "domain": "weather",
                    "metric": metric_name,
                    "value": metric_value,
                    "unit": units.get(api_metric, ""),
                    "source": "open-meteo",
                    "ingested_at": now_iso,
                }
            )

    return records


def parse_hourly_air_quality(city: str, latitude: float, longitude: float, payload: dict[str, Any]) -> list[dict[str, Any]]:
    hourly = payload.get("hourly", {})
    units = payload.get("hourly_units", {})
    times = hourly.get("time", [])
    if not times:
        return []

    metric_map = {
        "pm2_5": "pm2_5",
        "pm10": "pm10",
        "ozone": "ozone",
        "nitrogen_dioxide": "nitrogen_dioxide",
        "sulphur_dioxide": "sulphur_dioxide",
    }
    now_iso = datetime.now(timezone.utc).isoformat()
    records: list[dict[str, Any]] = []

    for idx, event_time in enumerate(times):
        for api_metric, metric_name in metric_map.items():
            series = hourly.get(api_metric, [])
            if idx >= len(series):
                continue
            metric_value = _safe_float(series[idx])
            if metric_value is None:
                continue
            records.append(
                {
                    "event_time": event_time,
                    "city": city,
                    "latitude": latitude,
                    "longitude": longitude,
                    "domain": "air_quality",
                    "metric": metric_name,
                    "value": metric_value,
                    "unit": units.get(api_metric, ""),
                    "source": "open-meteo-air-quality",
                    "ingested_at": now_iso,
                }
            )

    return records


def _build_api_params(parser_name: str, latitude: float, longitude: float) -> dict[str, Any]:
    if parser_name == "parse_current_weather":
        return {
            "latitude": latitude,
            "longitude": longitude,
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation",
            "timezone": "UTC",
        }
    if parser_name == "parse_hourly_weather":
        return {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation,surface_pressure",
            "past_days": 3,
            "forecast_days": 1,
            "timezone": "UTC",
        }
    if parser_name == "parse_hourly_air_quality":
        return {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": "pm2_5,pm10,ozone,nitrogen_dioxide,sulphur_dioxide",
            "past_days": 3,
            "forecast_days": 1,
            "timezone": "UTC",
        }
    raise ValueError(f"Unsupported parser '{parser_name}'")


def _create_clickhouse_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database=CLICKHOUSE_DATABASE,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )


def _ensure_table(client, table_name: str) -> None:
    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            event_time DateTime,
            city String,
            latitude Float32,
            longitude Float32,
            domain String,
            metric String,
            value Float32,
            unit String,
            source String,
            ingested_at DateTime
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(event_time)
        ORDER BY (city, event_time, metric)
        """
    )


def _iso_to_dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def produce_to_kafka(pipeline_key: str, **context) -> int:
    config = PIPELINES[pipeline_key]
    parser = globals()[config["parser"]]
    run_id = context["run_id"]

    producer = Producer({"bootstrap.servers": KAFKA_BROKER})
    produced_count = 0

    for city, (lat, lon) in CITIES.items():
        params = _build_api_params(config["parser"], lat, lon)
        payload = _fetch_json(config["api_url"], params)
        records = parser(city, lat, lon, payload)

        for record in records:
            record["run_id"] = run_id
            producer.produce(
                config["topic"],
                key=city.encode("utf-8"),
                value=json.dumps(record).encode("utf-8"),
            )
            produced_count += 1

    producer.flush(30)
    return produced_count


def _write_clickhouse(records: list[dict[str, Any]], table_name: str) -> None:
    client = _create_clickhouse_client()
    _ensure_table(client, table_name)

    rows = [
        [
            _iso_to_dt(record["event_time"]),
            record["city"],
            float(record["latitude"]),
            float(record["longitude"]),
            record["domain"],
            record["metric"],
            float(record["value"]),
            record.get("unit", ""),
            record["source"],
            _iso_to_dt(record["ingested_at"]),
        ]
        for record in records
    ]

    client.insert(
        table_name,
        rows,
        column_names=[
            "event_time",
            "city",
            "latitude",
            "longitude",
            "domain",
            "metric",
            "value",
            "unit",
            "source",
            "ingested_at",
        ],
    )


def _write_delta(records: list[dict[str, Any]], delta_path: str) -> None:
    df = pd.DataFrame(records)
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
    df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True)
    write_deltalake(delta_path, df, mode="append", partition_by=["city", "domain"])


def consume_kafka_and_load(pipeline_key: str, producer_task_id: str, **context) -> None:
    config = PIPELINES[pipeline_key]
    run_id = context["run_id"]
    expected_count = int(context["ti"].xcom_pull(task_ids=producer_task_id) or 0)
    if expected_count == 0:
        return

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": f"{pipeline_key}-loader-group",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([config["topic"]])

    records: list[dict[str, Any]] = []
    deadline = time.time() + 180
    while len(records) < expected_count and time.time() < deadline:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        record = json.loads(msg.value().decode("utf-8"))
        if record.get("run_id") != run_id:
            continue
        records.append(record)

    if records:
        _write_clickhouse(records, config["table"])
        _write_delta(records, config["delta_path"])
        consumer.commit(asynchronous=False)

    consumer.close()


def _build_dag(pipeline_key: str, schedule: str):
    config = PIPELINES[pipeline_key]

    with DAG(
        dag_id=config["dag_id"],
        description=f"{pipeline_key}: API -> Kafka -> ClickHouse + Delta",
        start_date=datetime(2026, 1, 1),
        schedule=schedule,
        catchup=False,
        tags=["course-project", "kafka", config["domain"]],
    ) as dag:
        produce = PythonOperator(
            task_id=f"produce_{pipeline_key}_to_kafka",
            python_callable=produce_to_kafka,
            op_kwargs={"pipeline_key": pipeline_key},
        )

        load = PythonOperator(
            task_id=f"consume_{pipeline_key}_and_load",
            python_callable=consume_kafka_and_load,
            op_kwargs={
                "pipeline_key": pipeline_key,
                "producer_task_id": f"produce_{pipeline_key}_to_kafka",
            },
        )

        produce >> load

    return dag


current_weather_dag = _build_dag("current_weather_pipeline", "0 6 * * *")
hourly_weather_dag = _build_dag("hourly_weather_pipeline", "@daily")
air_quality_dag = _build_dag("air_quality_pipeline", "@daily")
