# Smart Grid Energy Monitoring & Billing

**EC8203 – Applied Big Data Engineering — Mini Project**
**Use Case 3: Smart Grid Energy Monitoring & Billing**
Author: Tharindu — University of Ruhuna, Faculty of Engineering

An end-to-end **Lambda architecture** pipeline that gives a utility company real-time
visibility into grid load and solar (renewable) contribution from smart meters, and
reconciles that consumption daily against tariff/billing data to produce a per-household
bill and a zone-level solar report.

> Full architecture rationale, diagrams and results are in `report/SmartGrid_Report.pdf`.
> This README covers what the code does and how to run it.

---

## 1. Architecture at a glance

```
                         ┌────────────────────────────────────────────┐
                         │                 KAFKA                       │
  smart-meter            │  topic: meter-readings (6 partitions)       │
  simulator  ───produce──▶                                             │
  (streaming)             └───────────────┬──────────────────┬─────────┘
                                           │                  │
                              SPEED LAYER  │      BATCH LAYER │ (raw events archived)
                          (Spark Structured Streaming)        │
                                           │                  ▼
                                           ▼          data/raw/meter_readings/*.parquet
                              Postgres: realtime_zone_stats,        │
                              realtime_household_latest,            │
                              alerts                                │
                                           │                        ▼
                                           │              Airflow DAG (daily, batch layer)
                                           │              1. wait for tariff file
                                           │              2. Spark batch job:
                                           │                 join raw meter parquet (the day)
  tariff-billing simulator ──drops file──▶ │                 with tariff/weather CSV
  (batch, once/day)                        │                 -> household_bills
                                           │                 -> zone_solar_daily
                                           │                 -> data quality checks
                                           │              3. loads results into Postgres
                                           │                    (serving layer, batch views)
                                           ▼
                              ┌─────────────────────────┐
                              │   FastAPI serving layer  │
                              │  /zones/live             │
                              │  /households/{id}/bill   │
                              │  /alerts                 │
                              │  /metrics (Prometheus)   │
                              │  /health                 │
                              └─────────────────────────┘
                                           │
                                    dashboard.html (auto-refreshing)
```

* **Speed layer** — Spark Structured Streaming consumes `meter-readings` from Kafka
  continuously, computes 1-minute sliding-window (30 s slide) zone aggregates (load, solar mix) and
  per-household running totals, and upserts them into Postgres for instant querying. It
  also archives every raw event to Parquet (`data/raw/meter_readings/`) — this is what
  makes the batch layer possible without re-reading Kafka.
* **Batch layer** — Once a day (simulated day = 5 minutes), Airflow triggers a Spark
  batch job that re-processes the **full day's** raw Parquet events, joins them with that
  day's tariff/billing + weather-forecast file, and (re)computes the authoritative
  `household_bills` and `zone_solar_daily` tables. Because it recomputes from raw data
  rather than patching the streaming output, it can correct any errors/late data in the
  speed layer's approximate view — this is the classic Lambda "batch corrects speed"
  pattern.
* **Serving layer** — FastAPI reads both the speed-layer tables (for "right now") and the
  batch-layer tables (for "yesterday's official bill") from Postgres, merging them into a
  single consistent view per household/zone. `/metrics` exposes Prometheus counters and
  gauges; `/health` is the liveness/health-check endpoint Airflow and the alerting rule
  use.

See `report/SmartGrid_Report.pdf` §3 for the full Lambda-vs-Kappa justification.

## 2. Repository layout

```
smartgrid-project/
├── sources/                     # simulated data sources (Project Req. #1)
│   ├── meter_simulator.py       # streaming: Kafka producer, meter-readings every 2s
│   ├── tariff_batch_source.py   # daily-batch: drops tariff/weather CSV + calls webhook
│   └── common.py                # shared config: households, zones, simulated clock
├── spark/
│   ├── streaming_job.py         # speed layer: Structured Streaming, windowed agg
│   ├── batch_job.py             # batch layer: daily join + billing computation
│   └── requirements.txt
├── airflow/
│   └── dags/
│       └── smart_grid_batch_dag.py   # orchestration: sensor -> spark batch -> load -> checks -> alert
├── api/
│   ├── main.py                  # FastAPI serving layer + Prometheus metrics
│   ├── db.py                    # Postgres access
│   ├── schema.sql               # all table DDL
│   └── requirements.txt
├── dashboard/
│   └── dashboard.html           # single-page auto-refreshing dashboard (calls the API)
├── docker/
│   └── postgres/init.sql        # applied automatically on first Postgres boot
├── tests/                       # pytest unit tests for pure transformation/billing logic
│   ├── test_billing_logic.py
│   └── test_simulators.py
├── docker-compose.yml           # Kafka, Zookeeper, Spark, Postgres, Airflow, API — one command
├── .env.example
└── report/
    ├── SmartGrid_Report.docx / .pdf
    └── demo_speech.md
```

## 3. Technology stack (short version — see report for full justification)

| Layer | Choice | Why |
|---|---|---|
| Ingestion | **Apache Kafka** | Decouples the meter simulator from processing; partitioned by `grid_zone` so Spark can parallelize per zone; durable replay buffer feeds both speed and (via archiving) batch layers. |
| Stream processing | **Spark Structured Streaming** | Native windowed aggregation, exactly-once sink via checkpointing, and — critically — the **same DataFrame API and codebase style** is reused for the nightly batch job, cutting logic duplication between the two layers. |
| Orchestration | **Apache Airflow** | Daily DAG with a file sensor, retries, SLA/alerting, and a clear audit trail of each day's batch run — exactly what the coursework's "batch jobs / reporting pipeline" orchestration requirement asks for. |
| Storage/Sink | **PostgreSQL** (serving) + **Parquet on local FS** (raw/batch layer) | Postgres gives the API cheap indexed point/range queries (`household_id`, `zone`, time-range) needed for the REST layer; Parquet is the cheap, replayable, columnar store for the immutable master dataset the batch layer recomputes from. |
| Serving | **FastAPI** | Lightweight REST + built-in OpenAPI docs + easy Prometheus metrics middleware. |
| Observability | **structlog (JSON logs) + Prometheus client + Airflow email/log alert + `/health`** | See report §5. |

## 4. Prerequisites

* Docker Desktop / Docker Engine + Docker Compose v2 (`docker compose version`)
* ~6 GB free RAM for the containers (Kafka + Spark + Airflow + Postgres)
* VS Code with the **Dev Containers** or just the **Docker** and **Python** extensions (see
  §6 "Running from VS Code" below — no local Python/Kafka/Spark install is required)

## 5. Quick start (Docker Compose — recommended, matches the assignment brief)

```bash
# 1. clone / unzip the project, then from the project root:
cp .env.example .env

# 2. build and start every service (Kafka, Zookeeper, Postgres, Spark, Airflow, API)
docker compose up -d --build

# 3. wait ~60s for Kafka + Postgres to become healthy, then start the two simulators
docker compose up -d meter-simulator tariff-simulator

# 4. tail logs to watch the pipeline run
docker compose logs -f spark-streaming

# 5. open things in your browser
#    API docs:        http://localhost:8000/docs
#    Live dashboard:   http://localhost:8000/dashboard  (or open dashboard/dashboard.html)
#    Airflow UI:       http://localhost:8080  (user: admin / pass: admin)
#    Prometheus metrics: http://localhost:8000/metrics
```

Everything (simulated clock = **1 simulated day per 5 real minutes**) is defined in
`sources/common.py::SIM_DAY_SECONDS`. The first `tariff_20260901.csv` drops at start-up;
at the first day boundary (~5 minutes) the Airflow DAG fires for `20260901` and
`household_bills` populates about a minute later.

To stop and wipe all state:

```bash
docker compose down -v
```

## 6. Running the project from VS Code (step-by-step)

This is written for someone opening the project for the first time in VS Code.

1. **Open the folder** — `File → Open Folder…` → select `smartgrid-project/`.
2. **Install the recommended extensions** when VS Code prompts (or install manually from
   the Extensions panel, `Ctrl+Shift+X`):
   - `ms-azuretools.vscode-docker` (Docker)
   - `ms-python.python` (Python)
   - `ms-vscode-remote.remote-containers` (Dev Containers) — optional but convenient
3. **Copy the env file**: open the integrated terminal (`` Ctrl+` ``) and run:
   ```bash
   cp .env.example .env
   ```
4. **Start Docker Desktop** (if it is not already running) and confirm it's ready:
   ```bash
   docker info
   ```
5. **Build and launch the stack** from the same terminal:
   ```bash
   docker compose up -d --build
   ```
   Watch progress either in the terminal or in VS Code's **Docker extension panel**
   (left sidebar → whale icon), which lists all running containers
   (`kafka`, `zookeeper`, `postgres`, `spark-streaming`, `airflow-webserver`,
   `airflow-scheduler`, `api`).
6. **Start the two data simulators** as their own containers (kept separate so you can
   start/stop them independently while demoing):
   ```bash
   docker compose up -d meter-simulator tariff-simulator
   ```
7. **Watch it run**, right inside VS Code:
   - Right-click any container in the Docker panel → **View Logs**, or from the terminal:
     ```bash
     docker compose logs -f meter-simulator     # streaming events being produced
     docker compose logs -f spark-streaming     # speed-layer aggregation
     docker compose logs -f tariff-simulator    # daily file drop
     docker compose logs -f airflow-scheduler   # daily DAG runs
     ```
8. **Open the interfaces** — `Ctrl+click` these in the VS Code terminal to open in your
   browser, or use the **Simple Browser** (`Ctrl+Shift+P` → "Simple Browser: Show"):
   - API: `http://localhost:8000/docs`
   - Dashboard: `http://localhost:8000/dashboard`
   - Airflow: `http://localhost:8080`
9. **Run the unit tests** (they run without Docker, on pure billing/simulator logic):
   ```bash
   python3 -m venv .venv && source .venv/bin/activate
   pip install -r tests/requirements.txt
   pytest tests/ -v
   ```
   Or in VS Code: open the **Testing** panel (flask icon) → it auto-discovers
   `tests/test_*.py` via the `.vscode/settings.json` pytest configuration included in
   this repo → click ▶ to run/debug any test with breakpoints.
10. **Edit and hot-reload**: the Spark jobs and API are mounted as volumes
    (`docker-compose.yml`), so editing `api/main.py` or `spark/*.py` in VS Code and
    running `docker compose restart api` (or `spark-streaming`) picks up the change
    without a full rebuild.
11. **Shut everything down** when done:
    ```bash
    docker compose down -v
    ```

### Running components individually (no Docker) — for debugging in VS Code's debugger

If you only want to step through the **billing logic** or a **simulator** with VS Code's
debugger (`F5`), you don't need Kafka/Spark/Postgres at all:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r tests/requirements.txt
```

Then open `spark/batch_job.py` or `sources/meter_simulator.py`, set a breakpoint, and use
the included `.vscode/launch.json` configurations ("Python: Current File") to run with
`F5`. `meter_simulator.py` and `tariff_batch_source.py` also support a `--dry-run` flag
that prints events to stdout instead of requiring a live Kafka broker — handy for a quick
look at the data shape without starting Docker.

## 7. Observability

* **Structured logs** — every component (`sources/`, `spark/`, `api/`) logs JSON lines
  with `stage`, `event`, `timestamp`, and relevant IDs via `structlog`, so
  `docker compose logs` output is machine-parseable.
* **Health check** — `GET /health` reports `{status, kafka_last_message_age_s,
  db_connected, last_batch_run}`; it is what the alert rule below polls.
* **Alert rule #1 (streaming)** — the Spark streaming job raises a `low_renewable_alert`
  row in Postgres (and logs a `WARN`) whenever a zone's 1-minute solar share drops below
  `LOW_RENEWABLE_THRESHOLD` (default 10%), matching the brief's suggested output.
* **Alert rule #2 (freshness / health-check)** — `api/main.py` background task checks
  every 30s whether a `meter-readings` event has been seen in Postgres within the last
  `STALE_DATA_THRESHOLD_S` (default 90s); if not, it logs a `CRITICAL` "no data received"
  alert and flips `/health` to `degraded` — this satisfies the rubric's "no data received
  in N minutes" example directly.
* **Metrics** — `GET /metrics` exposes Prometheus counters/gauges
  (`api_requests_total`, `active_alerts`, `meter_data_age_seconds`, `pipeline_health`,
  `batch_job_last_success_timestamp`).
* Alerts are de-duplicated: the low-renewable rule fires once per (zone, window), and the
  no-data rule fires once when the stream goes stale (and logs `data_flow_recovered` when
  it comes back), so `active_alerts` reflects distinct incidents.

## 8. Simulated clock

* 1 simulated **day** = **5 minutes** of wall-clock time (`SIM_DAY_SECONDS = 300` in
  `sources/common.py`).
* Simulated day 0 is labelled `20260901`. Its start instant (the clock *epoch*) is written
  to `/data/sim_clock.json` on the shared volume by whichever simulator starts first; both
  simulators read it, so every meter reading carries a `sim_date` that matches the tariff
  file for the same simulated day. The speed layer partitions the raw Parquet archive by
  that `sim_date`. `docker compose down -v` resets the clock.
* The streaming simulator emits one reading per household roughly every **2 seconds**.
* At the start of every simulated day D the batch simulator drops `tariff_D.csv`
  (+ `weather_D.csv`), then (after a 20 s grace period so the speed layer's last
  micro-batch is archived) calls the Airflow REST API to trigger the DAG for **D-1**, the
  day that has just finished. The first bills therefore appear roughly 6–7 minutes after
  start-up.
* All of this is configurable via `.env` — see `.env.example`.

## 9. Limitations & what changes at production scale

See report §7 for the full discussion; briefly: single-broker Kafka and single-node Spark
here would become a multi-broker cluster with replication and a YARN/Kubernetes Spark
cluster; Postgres would need partitioning/TimescaleDB for the raw event volume a real grid
produces; and secrets/credentials would move out of `.env` into a proper secrets manager.
