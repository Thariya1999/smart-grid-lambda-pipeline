#!/usr/bin/env python3
"""
Daily-batch data source: tariff/billing + weather-forecast file drop.

At the start of every simulated day D (SIM_DAY_SECONDS, default 300s = 5min,
measured from the shared simulated-clock epoch in SIM_CLOCK_FILE), this script:
  1. Writes `tariff_<D>.csv` (household_id, tariff_rate, billing_tier,
     subsidy_flag) and `weather_<D>.csv` (grid_zone, date, cloud_cover,
     forecast_summary) into BATCH_DATA_DIR -- the reference data for day D.
  2. Calls the Airflow REST API to trigger the batch DAG for day D-1, which
     has just ended: all of D-1's meter readings are now in the raw archive
     and D-1's tariff file was dropped a day earlier, so the bill for
     "yesterday" can be computed -- the same way a production system would
     react to an SFTP drop / vendor API call.

Run standalone for a quick look without Airflow:
    python3 sources/tariff_batch_source.py --dry-run --days 1
"""
from __future__ import annotations

import argparse
import csv
import os
import random
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, "/app") if "/app" not in sys.path else None
from sources.common import (  # noqa: E402
    AIRFLOW_API_BASE,
    AIRFLOW_PASSWORD,
    AIRFLOW_USER,
    BATCH_DATA_DIR,
    GRID_ZONES,
    HOUSEHOLDS,
    SIM_CLOCK_FILE,
    SIM_DAY_SECONDS,
    get_logger,
    get_sim_epoch,
    log_event,
    sim_date_str,
    sim_day_index,
)
from sources.simulation_logic import generate_tariff_row, generate_weather_row  # noqa: E402

logger = get_logger("tariff-batch-source")

# Seconds to wait after a day boundary before triggering the previous day's
# batch run, so the speed layer's last micro-batch (10s trigger) of that day
# has been flushed to the raw Parquet archive.
BATCH_TRIGGER_GRACE_S = int(os.environ.get("BATCH_TRIGGER_GRACE_S", "20"))


def write_daily_files(day_index: int, out_dir: str, rng: random.Random) -> tuple[str, str]:
    os.makedirs(out_dir, exist_ok=True)
    date_str = sim_date_str(day_index)

    tariff_path = os.path.join(out_dir, f"tariff_{date_str}.csv")
    with open(tariff_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["household_id", "tariff_rate", "billing_tier", "subsidy_flag"])
        writer.writeheader()
        for h in HOUSEHOLDS:
            writer.writerow(generate_tariff_row(h, rng=rng))

    weather_path = os.path.join(out_dir, f"weather_{date_str}.csv")
    with open(weather_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["grid_zone", "date", "cloud_cover", "forecast_summary"])
        writer.writeheader()
        for zone in GRID_ZONES:
            writer.writerow(generate_weather_row(zone, date_str, rng=rng))

    return tariff_path, weather_path


def trigger_airflow_dag(date_str: str, attempts: int = 5) -> None:
    import requests

    logical_date = f"{date_str[0:4]}-{date_str[4:6]}-{date_str[6:8]}T00:00:00Z"
    for attempt in range(1, attempts + 1):
        try:
            resp = requests.post(
                f"{AIRFLOW_API_BASE}/dags/smart_grid_batch_dag/dagRuns",
                json={"logical_date": logical_date, "conf": {"sim_date": date_str}},
                auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
                timeout=10,
            )
            if resp.status_code in (200, 201):
                log_event(logger, "info", "airflow_dag_triggered", stage="ingestion",
                          sim_date=date_str, status=resp.status_code)
                return
            if resp.status_code == 409:  # a run for this logical date already exists
                log_event(logger, "info", "airflow_dag_run_already_exists", sim_date=date_str)
                return
            log_event(logger, "warning", "airflow_dag_trigger_failed", sim_date=date_str,
                      attempt=attempt, status=resp.status_code, body=resp.text[:300])
        except Exception as exc:  # noqa: BLE001 -- Airflow may still be starting up
            log_event(logger, "warning", "airflow_dag_trigger_error", sim_date=date_str,
                      attempt=attempt, error=str(exc))
        time.sleep(10)
    log_event(logger, "error", "airflow_dag_trigger_gave_up", sim_date=date_str, attempts=attempts)


def run(dry_run: bool, days: int | None) -> None:
    rng = random.Random(7)
    out_dir = "/tmp/dry_run_batch" if dry_run else BATCH_DATA_DIR
    sim_epoch = get_sim_epoch(None if dry_run else SIM_CLOCK_FILE)
    # Join the shared clock wherever it currently is (the meter simulator may
    # have started it earlier); dry-run just starts at day 0.
    day_index = 0 if dry_run else sim_day_index(sim_epoch)
    log_event(logger, "info", "sim_clock_loaded", sim_epoch=sim_epoch.isoformat(),
              sim_day_seconds=SIM_DAY_SECONDS, current_sim_date=sim_date_str(day_index))
    produced = 0

    while days is None or produced < days:
        date_str = sim_date_str(day_index)
        tariff_path, weather_path = write_daily_files(day_index, out_dir, rng)
        log_event(
            logger, "info", "daily_batch_file_dropped",
            stage="ingestion", sim_date=date_str, tariff_path=tariff_path, weather_path=weather_path,
            household_count=len(HOUSEHOLDS),
        )

        if not dry_run and day_index > 0:
            prev_date = sim_date_str(day_index - 1)
            if os.path.exists(os.path.join(out_dir, f"tariff_{prev_date}.csv")):
                time.sleep(BATCH_TRIGGER_GRACE_S)
                trigger_airflow_dag(prev_date)
            else:
                # e.g. this simulator started mid-way through the clock: the
                # previous day never had a tariff file, so it can't be billed.
                log_event(logger, "info", "previous_day_not_billable_no_tariff_file", sim_date=prev_date)

        produced += 1
        day_index += 1
        if days is None or produced < days:
            if dry_run:
                time.sleep(SIM_DAY_SECONDS)
            else:
                next_day_start = sim_epoch.timestamp() + day_index * SIM_DAY_SECONDS
                time.sleep(max(0.0, next_day_start - time.time()))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulated daily tariff/billing + weather batch source")
    parser.add_argument("--dry-run", action="store_true", help="Write to /tmp and skip triggering Airflow")
    parser.add_argument("--days", type=int, default=None, help="Stop after N simulated days")
    args = parser.parse_args()
    run(dry_run=args.dry_run, days=args.days)
