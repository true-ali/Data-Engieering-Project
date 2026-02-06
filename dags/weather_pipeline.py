from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

import clickhouse_connect
import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Consumer, Producer
from deltalake import write_deltalake

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather_raw")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "default")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

DELTA_TABLE_PATH = os.getenv("DELTA_TABLE_PATH", "/opt/airflow/lakehouse/weather_metrics")

# A small fixed set is enough for a minimal pipeline.
CITIES = {
    "Tehran": (35.6892, 51.3890),
    "Mashhad": (36.2605, 59.6168),
    "Shiraz": (29.5918, 52.5837),
}


def _fetch_weather_for_city(city: str, latitude: float, longitude: float) -> dict:
    response = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": latitude,
            "longitude": longitude,
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
            "timezone": "UTC",
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    current = payload["current"]
    return {
        "event_time": current["time"],
        "city": city,
        "latitude": latitude,
        "longitude": longitude,
        "temperature_c": current["temperature_2m"],
        "humidity_pct": current["relative_humidity_2m"],
        "wind_speed_kmh": current["wind_speed_10m"],
        "source": "open-meteo",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def produce_weather_to_kafka(**context) -> int:
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})
    produced_count = 0

    for city, (lat, lon) in CITIES.items():
        record = _fetch_weather_for_city(city, lat, lon)
        producer.produce(
            KAFKA_TOPIC,
            key=city.encode("utf-8"),
            value=json.dumps(record).encode("utf-8"),
        )
        produced_count += 1

    producer.flush(10)
    return produced_count


def _write_clickhouse(records: list[dict]) -> None:
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database=CLICKHOUSE_DATABASE,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )
    client.command(
        """
        CREATE TABLE IF NOT EXISTS weather_metrics (
            event_time DateTime,
            city String,
            latitude Float32,
            longitude Float32,
            temperature_c Float32,
            humidity_pct Float32,
            wind_speed_kmh Float32,
            source String,
            ingested_at DateTime
        )
        ENGINE = MergeTree
        ORDER BY (city, event_time)
        """
    )

    rows = []
    for record in records:
        rows.append(
            [
                datetime.fromisoformat(record["event_time"]),
                record["city"],
                float(record["latitude"]),
                float(record["longitude"]),
                float(record["temperature_c"]),
                float(record["humidity_pct"]),
                float(record["wind_speed_kmh"]),
                record["source"],
                datetime.fromisoformat(record["ingested_at"]),
            ]
        )

    client.insert(
        "weather_metrics",
        rows,
        column_names=[
            "event_time",
            "city",
            "latitude",
            "longitude",
            "temperature_c",
            "humidity_pct",
            "wind_speed_kmh",
            "source",
            "ingested_at",
        ],
    )


def _write_delta(records: list[dict]) -> None:
    df = pd.DataFrame(records)
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
    df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True)
    write_deltalake(DELTA_TABLE_PATH, df, mode="append")


def consume_kafka_and_load(**context) -> None:
    expected_count = int(context["ti"].xcom_pull(task_ids="produce_weather_to_kafka") or 0)
    if expected_count == 0:
        return

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": "weather-loader-group",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([KAFKA_TOPIC])

    records = []
    deadline = time.time() + 30
    while len(records) < expected_count and time.time() < deadline:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            continue
        records.append(json.loads(msg.value().decode("utf-8")))

    if records:
        _write_clickhouse(records)
        _write_delta(records)
        consumer.commit(asynchronous=False)

    consumer.close()


with DAG(
    dag_id="weather_to_kafka_clickhouse_delta",
    description="Daily weather pipeline: API -> Kafka -> ClickHouse + Delta Lake",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["course-project", "minimal"],
) as dag:
    produce = PythonOperator(
        task_id="produce_weather_to_kafka",
        python_callable=produce_weather_to_kafka,
    )

    load = PythonOperator(
        task_id="consume_kafka_and_load",
        python_callable=consume_kafka_and_load,
    )

    produce >> load
