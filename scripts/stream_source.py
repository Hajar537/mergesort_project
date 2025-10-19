#!/usr/bin/env python3
"""Utility for simulating a high-velocity streaming source.

The script appends JSON lines into ``data/stream/raw/events.jsonl`` at a
configurable interval.  Each invocation keeps track of the next event identifier
so that restarts continue producing unique records.
"""
from __future__ import annotations

import argparse
import json
import random
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List

DATA_DIR = Path("data/stream")
RAW_FILE = DATA_DIR / "raw" / "events.jsonl"
STATE_FILE = DATA_DIR / "source_state.json"
EVENT_TYPES = ["page_view", "add_to_cart", "purchase", "heartbeat"]


@dataclass
class Event:
    event_id: int
    event_type: str
    user_id: str
    value: float
    created_at: str


def load_state() -> int:
    if not STATE_FILE.exists():
        STATE_FILE.write_text(json.dumps({"next_event_id": 1}))
        return 1
    with STATE_FILE.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    return int(payload.get("next_event_id", 1))


def persist_state(next_event_id: int) -> None:
    STATE_FILE.write_text(json.dumps({"next_event_id": next_event_id}, indent=2))


def generate_events(start_id: int, batch_size: int) -> List[Event]:
    now = datetime.now(timezone.utc)
    events: List[Event] = []
    for offset in range(batch_size):
        event_id = start_id + offset
        event_type = random.choice(EVENT_TYPES)
        user_id = f"user_{random.randint(1, 50):03d}"
        value = round(random.uniform(1.0, 500.0), 2)
        events.append(
            Event(
                event_id=event_id,
                event_type=event_type,
                user_id=user_id,
                value=value,
                created_at=now.isoformat(),
            )
        )
    return events


def write_events(events: Iterable[Event]) -> None:
    RAW_FILE.parent.mkdir(parents=True, exist_ok=True)
    with RAW_FILE.open("a", encoding="utf-8") as fh:
        for event in events:
            fh.write(json.dumps(asdict(event)) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simulate a streaming data source")
    parser.add_argument("--interval", type=float, default=5.0, help="Sleep time between batches (seconds)")
    parser.add_argument("--batch-size", type=int, default=5, help="Number of events emitted per interval")
    parser.add_argument(
        "--iterations",
        type=int,
        default=0,
        help="Number of iterations to run (0 for infinite until interrupted)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    next_event_id = load_state()
    iteration = 0
    try:
        while True:
            iteration += 1
            events = generate_events(next_event_id, args.batch_size)
            write_events(events)
            next_event_id += args.batch_size
            persist_state(next_event_id)
            if 0 < args.iterations <= iteration:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
