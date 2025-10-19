"""Airflow DAG that ingests near-real-time weather observations via Weatherstack."""
from __future__ import annotations

import json
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

DATA_DIR = Path("data/stream")
RAW_FILE = DATA_DIR / "raw" / "weather_observations.jsonl"
STATE_FILE = DATA_DIR / "state.json"
PROCESSED_DIR = DATA_DIR / "processed"

WEATHERSTACK_ENDPOINT = "http://api.weatherstack.com/current"
WEATHERSTACK_API_KEY = "97b2240b1c8d6bd7260f340df5349956"
WEATHER_LOCATIONS = [
    "New York",
    "San Francisco",
    "London",
    "Tokyo",
]

ARGS = {
    "owner": "Hajar",
    "depends_on_past": False,
}


def load_state() -> Dict[str, Any]:
    if STATE_FILE.exists():
        with STATE_FILE.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    return {"locations": {}}


def persist_state(state: Dict[str, Any]) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


def fetch_weather_observation(api_key: str, location: str) -> Optional[Dict[str, Any]]:
    """Request the latest weather observation for ``location`` from Weatherstack."""

    try:
        response = requests.get(
            WEATHERSTACK_ENDPOINT,
            params={"access_key": api_key, "query": location},
            timeout=10,
        )
        response.raise_for_status()
    except requests.RequestException:
        return None

    try:
        payload = response.json()
    except ValueError:
        return None

    if not payload or payload.get("success") is False:
        return None

    return payload


def extract_weather(**context: Any) -> Dict[str, Any]:
    """Fetch new weather observations for configured locations using Weatherstack.

    The task calls the public Weatherstack API for each city listed in
    ``WEATHER_LOCATIONS``. The checkpoint stored in ``data/stream/state.json``
    retains the last observation timestamp per location so repeated DAG runs do
    not duplicate data when the upstream reading is unchanged.
    """

    api_key = os.environ.get("WEATHERSTACK_API_KEY", WEATHERSTACK_API_KEY)

    state = load_state()
    location_state: Dict[str, Dict[str, Any]] = state.get("locations", {})

    observations: List[Dict[str, Any]] = []
    raw_records: List[Dict[str, Any]] = []
    latest_markers: Dict[str, str] = {}

    for location in WEATHER_LOCATIONS:
        payload = fetch_weather_observation(api_key, location)
        if not payload:
            continue

        location_info = payload.get("location", {})
        current = payload.get("current", {})

        observed_at = location_info.get("localtime") or datetime.utcnow().isoformat()
        last_observed = (location_state.get(location) or {}).get("last_observed_at")

        if last_observed and observed_at <= last_observed:
            continue

        observation = {
            "location": location,
            "observed_at": observed_at,
            "temperature_c": current.get("temperature"),
            "feels_like_c": current.get("feelslike"),
            "humidity": current.get("humidity"),
            "wind_kph": current.get("wind_speed"),
            "weather_descriptions": current.get("weather_descriptions", []),
        }

        observations.append(observation)
        latest_markers[location] = observed_at
        raw_records.append(
            {
                "location": location,
                "observed_at": observed_at,
                "payload": payload,
            }
        )

    context["ti"].xcom_push(key="latest_markers", value=latest_markers)
    return {"observations": observations, "raw_records": raw_records}


def transform_weather(**context: Any) -> Dict[str, Any]:
    payload = context["ti"].xcom_pull(task_ids="extract_weather") or {}
    observations: List[Dict[str, Any]] = payload.get("observations", [])

    metrics: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"count": 0, "avg_temp_c": None})
    fleet_stats: Dict[str, Any] = {
        "observations": 0,
        "avg_temp_c": None,
        "avg_humidity": None,
    }

    running_temp = 0.0
    temp_count = 0
    running_humidity = 0.0
    humidity_count = 0

    for observation in observations:
        location = observation["location"]
        temperature = observation.get("temperature_c")
        humidity = observation.get("humidity")

        metrics_entry = metrics[location]
        metrics_entry["count"] += 1
        if temperature is not None:
            previous_total = (metrics_entry.get("avg_temp_c") or 0.0) * (metrics_entry["count"] - 1)
            metrics_entry["avg_temp_c"] = round((previous_total + float(temperature)) / metrics_entry["count"], 2)

        fleet_stats["observations"] += 1
        if temperature is not None:
            running_temp += float(temperature)
            temp_count += 1
        if humidity is not None:
            running_humidity += float(humidity)
            humidity_count += 1

    if temp_count:
        fleet_stats["avg_temp_c"] = round(running_temp / temp_count, 2)
    if humidity_count:
        fleet_stats["avg_humidity"] = round(running_humidity / humidity_count, 2)

    return {
        "observations": observations,
        "metrics": {"per_location": dict(metrics), "fleet": fleet_stats},
        "raw_records": payload.get("raw_records", []),
    }


def load_weather(**context: Any) -> None:
    ti = context["ti"]
    transformed_payload = ti.xcom_pull(task_ids="transform_weather") or {}
    latest_markers = ti.xcom_pull(key="latest_markers", task_ids="extract_weather") or {}

    observations = transformed_payload.get("observations", [])
    metrics = transformed_payload.get("metrics", {})
    raw_records = transformed_payload.get("raw_records", [])

    if latest_markers:
        state = load_state()
        state.setdefault("locations", {})
        for location, marker in latest_markers.items():
            state["locations"][location] = {"last_observed_at": marker}
        persist_state(state)

    if not observations:
        return

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    RAW_FILE.parent.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    if raw_records:
        with RAW_FILE.open("a", encoding="utf-8") as fh:
            for record in raw_records:
                fh.write(json.dumps(record) + "\n")

    output_file = PROCESSED_DIR / f"weather_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.json"
    with output_file.open("w", encoding="utf-8") as fh:
        json.dump({"observations": observations, "metrics": metrics}, fh, indent=2)


with DAG(
    dag_id="streaming_pipeline",
    description="Fetches fresh weather data via Weatherstack and stores incremental metrics",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/1 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["streaming", "demo"],
) as dag:
    extract_task = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_weather,
    )

    transform_task = PythonOperator(
        task_id="transform_weather",
        python_callable=transform_weather,
    )

    load_task = PythonOperator(
        task_id="load_weather",
        python_callable=load_weather,
    )

    extract_task >> transform_task >> load_task
