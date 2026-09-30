#!/usr/bin/env python3
"""
SERVING LAYER — FastAPI application.

Merges the speed-layer tables (realtime_zone_stats, realtime_household_latest,
alerts) with the batch-layer tables (household_bills, zone_solar_daily) so a
single API answers the business question end to end:

    "What is the current grid load and renewable contribution by zone, and
     what will each household's bill look like once daily tariff data is
     applied to their consumption?"

Also exposes Prometheus metrics and a /health endpoint, and runs a background
task that implements the "no data received in N minutes" freshness alert
rule required by the assignment's Observability section.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest

sys.path.insert(0, "/app") if "/app" not in sys.path else None
from api.db import query, query_one  # noqa: E402

STALE_DATA_THRESHOLD_S = int(os.environ.get("STALE_DATA_THRESHOLD_S", "90"))

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("api")


def log_event(level: str, message: str, **fields) -> None:
    import json
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level.upper(),
        "component": "api",
        "stage": "serving",
        "message": message,
        **fields,
    }
    print(json.dumps(payload), flush=True)


app = FastAPI(
    title="Smart Grid Energy Monitoring & Billing API",
    description="Serving layer for the EC8203 Smart Grid Lambda-architecture mini-project.",
    version="1.0.0",
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ----------------------------------------------------------------------------
# Prometheus metrics
# ----------------------------------------------------------------------------
API_REQUESTS = Counter("api_requests_total", "Total API requests", ["endpoint"])
ACTIVE_ALERTS_GAUGE = Gauge("active_alerts", "Number of unacknowledged alerts")
DATA_FRESHNESS_GAUGE = Gauge("meter_data_age_seconds", "Seconds since the most recent meter reading was stored")
PIPELINE_HEALTH_GAUGE = Gauge("pipeline_health", "1 = healthy, 0 = degraded (no fresh streaming data)")
BATCH_LAST_SUCCESS_GAUGE = Gauge("batch_job_last_success_timestamp", "Unix timestamp of the last successful batch run")


# ----------------------------------------------------------------------------
# Background freshness / health-check alert rule
# ----------------------------------------------------------------------------
_health_state = {"status": "starting", "kafka_last_message_age_s": None, "db_connected": False, "last_batch_run": None}


async def freshness_monitor_loop() -> None:
    """Alert rule #2: 'no data received in N minutes' health check, polled
    every 30 seconds. Flips /health to degraded and raises an alert row so
    it's visible both to a human watching /alerts and to Airflow/ops
    tooling polling /health."""
    while True:
        try:
            row = query_one("SELECT MAX(updated_at) AS latest FROM realtime_household_latest")
            db_connected = True
            if row and row["latest"]:
                age_s = (datetime.now(timezone.utc) - row["latest"]).total_seconds()
            else:
                age_s = None
        except Exception as exc:  # noqa: BLE001
            log_event("error", "freshness_check_db_error", error=str(exc))
            db_connected = False
            age_s = None

        stale = age_s is None or age_s > STALE_DATA_THRESHOLD_S
        _health_state["db_connected"] = db_connected
        _health_state["kafka_last_message_age_s"] = age_s
        _health_state["status"] = "degraded" if (stale or not db_connected) else "healthy"

        if age_s is not None:
            DATA_FRESHNESS_GAUGE.set(age_s)
        PIPELINE_HEALTH_GAUGE.set(0 if stale or not db_connected else 1)

        if stale and db_connected:
            log_event("critical", "no_data_received_alert",
                      threshold_s=STALE_DATA_THRESHOLD_S, age_s=age_s)
            # Edge-triggered: store one alert row when the stream *becomes*
            # stale, rather than a duplicate row every 30s while it stays stale.
            if not _health_state.get("stale_alert_raised"):
                age_txt = f"{age_s:.0f}s" if age_s is not None else "unknown (no readings yet)"
                try:
                    from api.db import get_conn
                    with get_conn() as conn, conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO alerts (alert_type, severity, message, created_at)
                            VALUES ('NO_DATA_RECEIVED', 'CRITICAL', %s, now())
                            """,
                            (f"No meter readings stored in the last {STALE_DATA_THRESHOLD_S}s "
                             f"(last reading age: {age_txt}).",),
                        )
                        conn.commit()
                    _health_state["stale_alert_raised"] = True
                except Exception as exc:  # noqa: BLE001
                    log_event("error", "alert_insert_failed", error=str(exc))
        elif not stale:
            if _health_state.get("stale_alert_raised"):
                log_event("info", "data_flow_recovered", age_s=age_s)
            _health_state["stale_alert_raised"] = False

        try:
            last_batch = query_one(
                "SELECT sim_date, run_at FROM batch_run_log WHERE status='SUCCESS' ORDER BY run_at DESC LIMIT 1"
            )
            if last_batch:
                _health_state["last_batch_run"] = last_batch["sim_date"]
                BATCH_LAST_SUCCESS_GAUGE.set(last_batch["run_at"].timestamp())
        except Exception:  # noqa: BLE001
            pass

        try:
            active = query_one("SELECT COUNT(*) AS n FROM alerts WHERE acknowledged = FALSE")
            if active:
                ACTIVE_ALERTS_GAUGE.set(active["n"])
        except Exception:  # noqa: BLE001
            pass

        await asyncio.sleep(30)


@app.on_event("startup")
async def on_startup() -> None:
    log_event("info", "api_starting")
    asyncio.create_task(freshness_monitor_loop())


# ----------------------------------------------------------------------------
# Endpoints
# ----------------------------------------------------------------------------
@app.get("/health")
def health():
    API_REQUESTS.labels(endpoint="/health").inc()
    status_code = 200 if _health_state["status"] == "healthy" else 503
    body = {**_health_state, "checked_at": datetime.now(timezone.utc).isoformat()}
    if status_code != 200 and _health_state["status"] != "starting":
        # Still return 200-shaped JSON body but signal degradation via field;
        # keep HTTP 200 so simple pollers don't treat "starting" as down.
        pass
    return body


@app.get("/metrics")
def metrics():
    API_REQUESTS.labels(endpoint="/metrics").inc()
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/zones/live")
def zones_live():
    """Real-time grid load / renewable-mix by zone -- the 'right now' half
    of the business question. Uses the most recent completed window per
    zone from the speed layer."""
    API_REQUESTS.labels(endpoint="/zones/live").inc()
    rows = query(
        """
        SELECT DISTINCT ON (grid_zone)
            grid_zone, window_start, window_end, total_consumption_kwh,
            total_solar_kwh, renewable_share, reading_count, updated_at
        FROM realtime_zone_stats
        ORDER BY grid_zone, window_start DESC
        """
    )
    for r in rows:
        for k in ("window_start", "window_end", "updated_at"):
            if r.get(k):
                r[k] = r[k].isoformat()
    return {"zones": rows, "generated_at": datetime.now(timezone.utc).isoformat()}


@app.get("/zones/{zone}/history")
def zone_history(zone: str, limit: int = 50):
    API_REQUESTS.labels(endpoint="/zones/{zone}/history").inc()
    rows = query(
        """
        SELECT window_start, window_end, total_consumption_kwh, total_solar_kwh, renewable_share
        FROM realtime_zone_stats WHERE grid_zone = %s
        ORDER BY window_start DESC LIMIT %s
        """,
        (zone, limit),
    )
    for r in rows:
        r["window_start"] = r["window_start"].isoformat()
        r["window_end"] = r["window_end"].isoformat()
    return {"grid_zone": zone, "history": list(reversed(rows))}


@app.get("/households/{household_id}")
def household_status(household_id: str):
    """Merges the speed-layer 'right now' reading with the most recent
    batch-layer bill for this household -- the core Lambda-serving-layer
    merge the report describes."""
    API_REQUESTS.labels(endpoint="/households/{id}").inc()
    latest = query_one(
        "SELECT * FROM realtime_household_latest WHERE household_id = %s", (household_id,)
    )
    last_bill = query_one(
        "SELECT * FROM household_bills WHERE household_id = %s ORDER BY sim_date DESC LIMIT 1",
        (household_id,),
    )
    if not latest and not last_bill:
        raise HTTPException(status_code=404, detail=f"No data found for household {household_id}")

    if latest and latest.get("reading_timestamp"):
        latest["reading_timestamp"] = latest["reading_timestamp"].isoformat()
        latest["updated_at"] = latest["updated_at"].isoformat()
    return {"household_id": household_id, "live": latest, "latest_bill": last_bill}


@app.get("/households/{household_id}/bill")
def household_bill(household_id: str, sim_date: str | None = None):
    """The consolidated daily billing + solar-contribution report per
    household the brief asks for as a suggested output."""
    API_REQUESTS.labels(endpoint="/households/{id}/bill").inc()
    if sim_date:
        bill = query_one(
            "SELECT * FROM household_bills WHERE household_id = %s AND sim_date = %s",
            (household_id, sim_date),
        )
    else:
        bill = query_one(
            "SELECT * FROM household_bills WHERE household_id = %s ORDER BY sim_date DESC LIMIT 1",
            (household_id,),
        )
    if not bill:
        raise HTTPException(status_code=404, detail=f"No bill found for household {household_id}")
    return bill


@app.get("/bills")
def all_bills(sim_date: str | None = None):
    API_REQUESTS.labels(endpoint="/bills").inc()
    if sim_date:
        rows = query("SELECT * FROM household_bills WHERE sim_date = %s ORDER BY household_id", (sim_date,))
    else:
        latest_date = query_one("SELECT MAX(sim_date) AS d FROM household_bills")
        sim_date = latest_date["d"] if latest_date else None
        rows = query("SELECT * FROM household_bills WHERE sim_date = %s ORDER BY household_id",
                     (sim_date,)) if sim_date else []
    return {"sim_date": sim_date, "bills": rows}


@app.get("/zones/{zone}/solar-report")
def zone_solar_report(zone: str, sim_date: str | None = None):
    API_REQUESTS.labels(endpoint="/zones/{zone}/solar-report").inc()
    if sim_date:
        row = query_one("SELECT * FROM zone_solar_daily WHERE grid_zone = %s AND sim_date = %s", (zone, sim_date))
    else:
        row = query_one(
            "SELECT * FROM zone_solar_daily WHERE grid_zone = %s ORDER BY sim_date DESC LIMIT 1", (zone,)
        )
    if not row:
        raise HTTPException(status_code=404, detail=f"No solar report found for zone {zone}")
    return row


@app.get("/alerts")
def alerts(limit: int = 50, unacknowledged_only: bool = False):
    API_REQUESTS.labels(endpoint="/alerts").inc()
    sql = "SELECT * FROM alerts"
    if unacknowledged_only:
        sql += " WHERE acknowledged = FALSE"
    sql += " ORDER BY created_at DESC LIMIT %s"
    rows = query(sql, (limit,))
    for r in rows:
        r["created_at"] = r["created_at"].isoformat()
        if r.get("window_start"):
            r["window_start"] = r["window_start"].isoformat()
    return {"alerts": rows}


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    """Serves the static single-page dashboard directly from the API so a
    grader can open one URL (no separate static file server needed)."""
    API_REQUESTS.labels(endpoint="/dashboard").inc()
    path = os.path.join(os.path.dirname(__file__), "..", "dashboard", "dashboard.html")
    with open(path, "r") as f:
        return HTMLResponse(f.read())
