"""
Shared configuration & helpers for the simulated data sources.

Both the streaming (smart-meter) simulator and the daily-batch
(tariff/billing) simulator import this module so that the two sources stay
consistent with each other (same households, same zones, same simulated
clock) -- exactly what lets the batch layer later JOIN them correctly.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone

# --------------------------------------------------------------------------
# Simulated clock
# --------------------------------------------------------------------------
# 1 "simulated day" = SIM_DAY_SECONDS of wall-clock time.
# Default 300s (5 minutes) so the whole multi-day pipeline can be
# demonstrated inside one session, per the assignment brief.
SIM_DAY_SECONDS = int(os.environ.get("SIM_DAY_SECONDS", "300"))

# --------------------------------------------------------------------------
# Fixed "world" -- households, meters, zones.
# Small and deterministic on purpose: makes the demo/report reproducible.
# --------------------------------------------------------------------------
GRID_ZONES = ["ZONE-A", "ZONE-B", "ZONE-C", "ZONE-D"]

BILLING_TIERS = ["RESIDENTIAL", "RESIDENTIAL_SUBSIDIZED", "SMALL_BUSINESS"]

TARIFF_RATE_BY_TIER = {
    # LKR per kWh -- illustrative, loosely based on Sri Lankan domestic tariff bands
    "RESIDENTIAL": 45.0,
    "RESIDENTIAL_SUBSIDIZED": 30.0,
    "SMALL_BUSINESS": 60.0,
}


@dataclass(frozen=True)
class Household:
    household_id: str
    meter_id: str
    grid_zone: str
    has_solar: bool
    billing_tier: str


def build_households(n_per_zone: int = 5) -> list[Household]:
    """Deterministically build the fixed population of simulated households."""
    households: list[Household] = []
    hh_counter = 1
    for zone in GRID_ZONES:
        for i in range(n_per_zone):
            hh_id = f"HH-{hh_counter:04d}"
            meter_id = f"MTR-{hh_counter:04d}"
            has_solar = (hh_counter % 3 == 0)  # ~1/3 of households have rooftop solar
            tier = BILLING_TIERS[hh_counter % len(BILLING_TIERS)]
            households.append(Household(hh_id, meter_id, zone, has_solar, tier))
            hh_counter += 1
    return households


HOUSEHOLDS: list[Household] = build_households()


def household_by_id(household_id: str) -> Household | None:
    for h in HOUSEHOLDS:
        if h.household_id == household_id:
            return h
    return None


# --------------------------------------------------------------------------
# Simulated-time helpers
# --------------------------------------------------------------------------
def sim_day_index(start_time: datetime, now: datetime | None = None) -> int:
    """Which simulated day (0-indexed) `now` falls in, relative to start_time."""
    now = now or datetime.now(timezone.utc)
    elapsed = (now - start_time).total_seconds()
    return max(0, int(elapsed // SIM_DAY_SECONDS))


def sim_date_str(day_index: int, start_date: str = "2026-09-01") -> str:
    """Map a simulated day index onto a human-readable calendar date string,
    purely for labeling output files / partitions (e.g. tariff_20260903.csv)."""
    from datetime import date, timedelta
    y, m, d = (int(x) for x in start_date.split("-"))
    return (date(y, m, d) + timedelta(days=day_index)).strftime("%Y%m%d")


def get_sim_epoch(clock_file: str | None) -> datetime:
    """
    The shared "day 0 starts here" instant of the simulated clock.

    Both simulators run in separate containers, so they must agree on which
    simulated day it is -- otherwise the streaming events and the daily
    tariff files get different sim_date labels and the batch layer's join
    finds nothing. The first simulator to start writes the epoch to
    `clock_file` (on the shared /data volume) atomically via O_EXCL; every
    other process just reads it. `clock_file=None` (dry-run) uses "now".
    """
    now = datetime.now(timezone.utc)
    if clock_file is None:
        return now
    os.makedirs(os.path.dirname(clock_file), exist_ok=True)
    try:
        fd = os.open(clock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, "w") as f:
            json.dump({"sim_epoch": now.isoformat(), "sim_day_seconds": SIM_DAY_SECONDS}, f)
        return now
    except FileExistsError:
        # Another process may have created the file but not finished writing it yet.
        for _ in range(50):
            try:
                with open(clock_file) as f:
                    return datetime.fromisoformat(json.load(f)["sim_epoch"])
            except (json.JSONDecodeError, KeyError):
                import time
                time.sleep(0.1)
        raise RuntimeError(f"Could not read simulated clock epoch from {clock_file}")


def current_sim_date(epoch: datetime, now: datetime | None = None) -> str:
    """The YYYYMMDD label of the simulated day `now` falls in."""
    return sim_date_str(sim_day_index(epoch, now))


# --------------------------------------------------------------------------
# Structured (JSON) logging -- shared by every component for observability.
# --------------------------------------------------------------------------
def get_logger(component: str) -> logging.Logger:
    logger = logging.getLogger(component)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)

    class JsonFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            payload = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "level": record.levelname,
                "component": component,
                "message": record.getMessage(),
            }
            if hasattr(record, "extra_fields"):
                payload.update(record.extra_fields)  # type: ignore[attr-defined]
            return json.dumps(payload)

    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    return logger


def log_event(logger: logging.Logger, level: str, message: str, **fields) -> None:
    """Log a structured event with arbitrary extra key/value fields."""
    record_level = getattr(logging, level.upper(), logging.INFO)
    logger.log(record_level, message, extra={"extra_fields": fields})


# --------------------------------------------------------------------------
# Kafka / env config shared across producers & consumers
# --------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
METER_TOPIC = os.environ.get("METER_TOPIC", "meter-readings")

BATCH_DATA_DIR = os.environ.get("BATCH_DATA_DIR", "/data/batch_incoming")
RAW_ARCHIVE_DIR = os.environ.get("RAW_ARCHIVE_DIR", "/data/raw/meter_readings")
SIM_CLOCK_FILE = os.environ.get("SIM_CLOCK_FILE", "/data/sim_clock.json")

AIRFLOW_API_BASE = os.environ.get("AIRFLOW_API_BASE", "http://airflow-webserver:8080/api/v1")
AIRFLOW_USER = os.environ.get("AIRFLOW_USER", "admin")
AIRFLOW_PASSWORD = os.environ.get("AIRFLOW_PASSWORD", "admin")

LOW_RENEWABLE_THRESHOLD = float(os.environ.get("LOW_RENEWABLE_THRESHOLD", "0.10"))
STALE_DATA_THRESHOLD_S = int(os.environ.get("STALE_DATA_THRESHOLD_S", "90"))
