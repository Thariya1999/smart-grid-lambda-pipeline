-- Smart Grid serving-layer schema.
-- Applied automatically on first Postgres container start (mounted into
-- /docker-entrypoint-initdb.d/ via docker-compose.yml).

-- ===========================================================================
-- SPEED LAYER tables -- written continuously by spark/streaming_job.py
-- ===========================================================================

-- Windowed (1-minute sliding, 30s slide) per-zone aggregates: the "grid
-- load and renewable contribution by zone, right now" view.
CREATE TABLE IF NOT EXISTS realtime_zone_stats (
    grid_zone               TEXT NOT NULL,
    window_start            TIMESTAMPTZ NOT NULL,
    window_end              TIMESTAMPTZ NOT NULL,
    total_consumption_kwh   DOUBLE PRECISION NOT NULL,
    total_solar_kwh         DOUBLE PRECISION NOT NULL,
    renewable_share         DOUBLE PRECISION NOT NULL,
    reading_count           INTEGER NOT NULL,
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (grid_zone, window_start)
);
CREATE INDEX IF NOT EXISTS idx_zone_stats_window ON realtime_zone_stats (window_start DESC);

-- Latest reading per household -- the "current status" point lookup and
-- the source of truth for the data-freshness/health-check alert rule.
CREATE TABLE IF NOT EXISTS realtime_household_latest (
    household_id            TEXT PRIMARY KEY,
    meter_id                TEXT NOT NULL,
    grid_zone                TEXT NOT NULL,
    power_consumption_kwh   DOUBLE PRECISION NOT NULL,
    solar_generation_kwh    DOUBLE PRECISION NOT NULL,
    reading_timestamp       TIMESTAMPTZ NOT NULL,
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Alerts raised by either the streaming job (low renewable share) or the
-- API's background freshness check (no data received).
CREATE TABLE IF NOT EXISTS alerts (
    id              SERIAL PRIMARY KEY,
    alert_type      TEXT NOT NULL,
    severity        TEXT NOT NULL,
    grid_zone       TEXT,
    household_id    TEXT,
    window_start    TIMESTAMPTZ,             -- streaming window that raised it (dedup key)
    message         TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    acknowledged    BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON alerts (created_at DESC);

-- ===========================================================================
-- BATCH LAYER tables -- written once/day by spark/batch_job.py via Airflow
-- ===========================================================================

-- The authoritative daily bill per household (the project's core "business
-- question" deliverable).
CREATE TABLE IF NOT EXISTS household_bills (
    household_id                TEXT NOT NULL,
    grid_zone                   TEXT NOT NULL,
    sim_date                    TEXT NOT NULL,
    total_consumption_kwh       DOUBLE PRECISION NOT NULL,
    total_solar_kwh             DOUBLE PRECISION NOT NULL,
    net_consumption_kwh         DOUBLE PRECISION NOT NULL,
    tariff_rate                 DOUBLE PRECISION NOT NULL,
    billing_tier                TEXT NOT NULL,
    subsidy_flag                BOOLEAN NOT NULL,
    gross_amount                DOUBLE PRECISION NOT NULL,
    subsidy_discount_pct        DOUBLE PRECISION NOT NULL,
    final_amount                DOUBLE PRECISION NOT NULL,
    solar_self_sufficiency_pct  DOUBLE PRECISION NOT NULL,
    reading_count                INTEGER NOT NULL,
    PRIMARY KEY (household_id, sim_date)
);
CREATE INDEX IF NOT EXISTS idx_household_bills_date ON household_bills (sim_date DESC);

-- Zone-level daily solar-contribution report.
CREATE TABLE IF NOT EXISTS zone_solar_daily (
    grid_zone               TEXT NOT NULL,
    sim_date                 TEXT NOT NULL,
    total_consumption_kwh   DOUBLE PRECISION NOT NULL,
    total_solar_kwh         DOUBLE PRECISION NOT NULL,
    renewable_share         DOUBLE PRECISION NOT NULL,
    household_count         INTEGER NOT NULL,
    cloud_cover              DOUBLE PRECISION,
    forecast_summary        TEXT,
    PRIMARY KEY (grid_zone, sim_date)
);

-- Bookkeeping table so the API's /health endpoint and Prometheus metrics
-- can report when the batch layer last completed successfully.
CREATE TABLE IF NOT EXISTS batch_run_log (
    id              SERIAL PRIMARY KEY,
    sim_date        TEXT NOT NULL,
    status          TEXT NOT NULL,           -- SUCCESS | FAILED
    detail          TEXT,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
