#!/usr/bin/env python3
"""
Streaming data source: simulated smart-meter Kafka producer.

Emits one reading per household roughly every METER_INTERVAL_SECONDS seconds,
to the `meter-readings` Kafka topic, keyed by grid_zone (so Spark's
per-partition parallelism aligns with the zone-level aggregation the speed
layer needs).

Run standalone for a quick look without Kafka:
    python3 sources/meter_simulator.py --dry-run --count 5

Run for real (inside docker-compose, KAFKA_BOOTSTRAP_SERVERS is already set):
    python3 sources/meter_simulator.py
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, "/app") if "/app" not in sys.path else None
from sources.common import (  # noqa: E402
    HOUSEHOLDS,
    KAFKA_BOOTSTRAP_SERVERS,
    METER_TOPIC,
    get_logger,
    log_event,
)
from sources.simulation_logic import generate_meter_reading  # noqa: E402

logger = get_logger("meter-simulator")

METER_INTERVAL_SECONDS = 2.0


def run(dry_run: bool, count: int | None) -> None:
    producer = None
    if not dry_run:
        from kafka import KafkaProducer
        from kafka.errors import NoBrokersAvailable

        retries = 0
        while producer is None:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    linger_ms=50,
                )
                log_event(logger, "info", "kafka_producer_connected",
                          bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
            except NoBrokersAvailable:
                retries += 1
                log_event(logger, "warning", "kafka_not_ready_retrying", attempt=retries)
                time.sleep(3)
                if retries > 40:
                    log_event(logger, "error", "kafka_unreachable_giving_up")
                    raise

    rng = random.Random(42)
    emitted = 0
    # Cloud cover drifts slowly to give the "low renewable" alert something
    # to occasionally trip on, independent of the daily weather file (which
    # only affects the *batch* solar report).
    cloud_cover = 0.1

    try:
        while count is None or emitted < count:
            cloud_cover = min(1.0, max(0.0, cloud_cover + rng.uniform(-0.05, 0.05)))
            now = datetime.now(timezone.utc)
            for household in HOUSEHOLDS:
                event = generate_meter_reading(household, now=now, cloud_cover=cloud_cover, rng=rng)
                if dry_run:
                    print(json.dumps(event))
                else:
                    producer.send(METER_TOPIC, key=household.grid_zone, value=event)
                emitted += 1
                if count is not None and emitted >= count:
                    break

            if not dry_run:
                producer.flush()
            log_event(
                logger, "info", "meter_batch_emitted",
                stage="ingestion", households=len(HOUSEHOLDS), cloud_cover=round(cloud_cover, 2),
                total_emitted=emitted,
            )
            time.sleep(METER_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        log_event(logger, "info", "meter_simulator_stopped_by_user")
    finally:
        if producer is not None:
            producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulated smart-meter streaming source")
    parser.add_argument("--dry-run", action="store_true", help="Print events instead of publishing to Kafka")
    parser.add_argument("--count", type=int, default=None, help="Stop after emitting N events (for --dry-run demos)")
    args = parser.parse_args()
    run(dry_run=args.dry_run, count=args.count)
