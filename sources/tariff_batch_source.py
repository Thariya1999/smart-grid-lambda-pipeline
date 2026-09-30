#!/usr/bin/env python3
"""
Daily-batch data source: tariff/billing + weather-forecast file drop.

Once per simulated day (SIM_DAY_SECONDS, default 300s = 5min), this script:
  1. Writes `tariff_<YYYYMMDD>.csv` (household_id, tariff_rate, billing_tier,
     subsidy_flag) and `weather_<YYYYMMDD>.csv` (grid_zone, date, cloud_cover,
     forecast_summary) into BATCH_DATA_DIR.
  2. Calls the Airflow REST API to trigger that day's batch DAG run, so the
     orchestration layer reacts to the new file the same way a production
     system would react to an SFTP drop / vendor API call.

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
    SIM_DAY_SECONDS,
    get_logger,
    log_event,
    sim_date_str,
)
from sources.simulation_logic import generate_tariff_row, generate_weather_row  # noqa: E402

logger = get_logger("tariff-batch-source")


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


def trigger_airflow_dag(date_str: str) -> None:
    try:
        import requests

        logical_date = f"{date_str[0:4]}-{date_str[4:6]}-{date_str[6:8]}T00:00:00Z"
        resp = requests.post(
            f"{AIRFLOW_API_BASE}/dags/smart_grid_batch_dag/dagRuns",
            json={"logical_date": logical_date, "conf": {"sim_date": date_str}},
            auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
            timeout=10,
        )
        if resp.status_code in (200, 201):
            log_event(logger, "info", "airflow_dag_triggered", sim_date=date_str, status=resp.status_code)
        else:
            log_event(logger, "warning", "airflow_dag_trigger_failed",
                      sim_date=date_str, status=resp.status_code, body=resp.text[:300])
    except Exception as exc:  # noqa: BLE001 -- best-effort; the file sensor is the real trigger
        log_event(logger, "warning", "airflow_dag_trigger_error", sim_date=date_str, error=str(exc))


def run(dry_run: bool, days: int | None) -> None:
    rng = random.Random(7)
    out_dir = "/tmp/dry_run_batch" if dry_run else BATCH_DATA_DIR
    day_index = 0

    while days is None or day_index < days:
        date_str = sim_date_str(day_index)
        tariff_path, weather_path = write_daily_files(day_index, out_dir, rng)
        log_event(
            logger, "info", "daily_batch_file_dropped",
            stage="ingestion", sim_date=date_str, tariff_path=tariff_path, weather_path=weather_path,
            household_count=len(HOUSEHOLDS),
        )
        if not dry_run:
            trigger_airflow_dag(date_str)

        day_index += 1
        if days is None or day_index < days:
            time.sleep(SIM_DAY_SECONDS)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulated daily tariff/billing + weather batch source")
    parser.add_argument("--dry-run", action="store_true", help="Write to /tmp and skip triggering Airflow")
    parser.add_argument("--days", type=int, default=None, help="Stop after N simulated days")
    args = parser.parse_args()
    run(dry_run=args.dry_run, days=args.days)
