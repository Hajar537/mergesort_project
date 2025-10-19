"""Airflow DAG orchestrating a scheduled batch processing pipeline."""
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

DATA_DIR = Path("data/batch")
RAW_FILE = DATA_DIR / "raw" / "weather_history.jsonl"
PROCESSED_DIR = DATA_DIR / "processed"
STATE_FILE = DATA_DIR / "state.json"

WEATHERSTACK_ENDPOINT = "http://api.weatherstack.com/current"
WEATHERSTACK_DEFAULT_KEY = "97b2240b1c8d6bd7260f340df5349956"
WEATHER_LOCATIONS = [
    "New York",
    "San Francisco",
    "London",
    "Tokyo",
]

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
}


def resolve_api_key() -> str:
    """Return the Weatherstack API key, preferring the environment variable."""

    return os.environ.get("WEATHERSTACK_API_KEY", WEATHERSTACK_DEFAULT_KEY)


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
    """Fetch new weather observations for each configured city."""

    api_key = resolve_api_key()
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


def transform_observations(**context: Any) -> Dict[str, Any]:
    payload = context["ti"].xcom_pull(task_ids="extract_weather") or {}
    observations: List[Dict[str, Any]] = payload.get("observations", [])

    metrics: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {"count": 0, "avg_temp_c": None, "avg_humidity": None}
    )

    for observation in observations:
        location = observation["location"]
        temperature = observation.get("temperature_c")
        humidity = observation.get("humidity")

        metrics_entry = metrics[location]
        metrics_entry["count"] += 1
        if temperature is not None:
            prev_total = (metrics_entry.get("avg_temp_c") or 0.0) * (metrics_entry["count"] - 1)
            metrics_entry["avg_temp_c"] = round((prev_total + float(temperature)) / metrics_entry["count"], 2)
        if humidity is not None:
            prev_humidity = (metrics_entry.get("avg_humidity") or 0.0) * (
                metrics_entry["count"] - 1
            )
            metrics_entry["avg_humidity"] = round(
                (prev_humidity + float(humidity)) / metrics_entry["count"], 2
            )

    return {
        "observations": observations,
        "metrics": dict(metrics),
        "raw_records": payload.get("raw_records", []),
    }


def load_results(**context: Any) -> None:
    ti = context["ti"]
    transformed_payload = ti.xcom_pull(task_ids="transform_observations") or {}
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

    output_path = PROCESSED_DIR / f"weather_batch_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.json"
    with output_path.open("w", encoding="utf-8") as fh:
        json.dump({"observations": observations, "metrics": metrics}, fh, indent=2)


with DAG(
    dag_id="batch_weather_pipeline",
    description="Processes scheduled Weatherstack snapshots and aggregates metrics",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 */2 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["batch", "demo"],
) as dag:
    extract_task = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_weather,
    )

    transform_task = PythonOperator(
        task_id="transform_observations",
        python_callable=transform_observations,
    )

    load_task = PythonOperator(
        task_id="load_results",
        python_callable=load_results,
    )

    extract_task >> transform_task >> load_task
