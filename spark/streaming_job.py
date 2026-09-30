#!/usr/bin/env python3
"""
SPEED LAYER — Spark Structured Streaming job.

Reads raw JSON events from the Kafka topic `meter-readings`, and for every
1-minute sliding window (30s slide) per grid_zone:
  * sums power consumption and solar generation
  * computes the zone's renewable (solar) share of load
  * flags a low-renewable alert when that share drops below threshold

It also:
  * maintains a "latest reading per household" table for point lookups
    (`GET /households/{id}` "right now" view)
  * archives every raw event, unmodified, to Parquet -- this is what allows
    the BATCH layer (spark/batch_job.py) to later recompute the
    authoritative daily numbers from the *same* raw data without needing to
    replay Kafka (Kafka's retention is short; Parquet is the long-term
    "master dataset" a Lambda architecture's batch layer expects).

Two independent `foreachBatch` sinks are used instead of `.format("jdbc")`
directly, because Spark's structured streaming JDBC sink support is
write-only/append-only per micro-batch; foreachBatch gives us upsert
(ON CONFLICT) semantics against Postgres, which is what a live "current
state per zone/household" table needs.
"""
from __future__ import annotations

import json
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
METER_TOPIC = os.environ.get("METER_TOPIC", "meter-readings")
RAW_ARCHIVE_DIR = os.environ.get("RAW_ARCHIVE_DIR", "/data/raw/meter_readings")
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/data/checkpoints/streaming_job")
LOW_RENEWABLE_THRESHOLD = float(os.environ.get("LOW_RENEWABLE_THRESHOLD", "0.10"))

PG_URL = os.environ.get("PG_JDBC_URL", "jdbc:postgresql://postgres:5432/smartgrid")
PG_HOST = os.environ.get("PG_HOST", "postgres")
PG_DB = os.environ.get("PG_DB", "smartgrid")
PG_USER = os.environ.get("PG_USER", "smartgrid")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "smartgrid")

METER_SCHEMA = StructType([
    StructField("meter_id", StringType()),
    StructField("household_id", StringType()),
    StructField("power_consumption_kwh", DoubleType()),
    StructField("solar_generation_kwh", DoubleType()),
    StructField("grid_zone", StringType()),
    StructField("timestamp", StringType()),
    StructField("sim_date", StringType()),  # simulated day label (YYYYMMDD) from the shared sim clock
])


def log(stage: str, event: str, **fields) -> None:
    """Structured JSON log line -- matches the format used by the Python
    sources/API components so `docker compose logs` is uniformly parseable."""
    payload = {"component": "spark-streaming", "stage": stage, "event": event, **fields}
    print(json.dumps(payload), flush=True)


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("SmartGridStreamingSpeedLayer")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )


def read_meter_stream(spark: SparkSession) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", METER_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = (
        raw.select(F.col("value").cast("string").alias("json_str"), F.col("timestamp").alias("kafka_ts"))
        .select(F.from_json(F.col("json_str"), METER_SCHEMA).alias("data"), "kafka_ts")
        .select("data.*", "kafka_ts")
        .withColumn("event_time", F.to_timestamp("timestamp"))
        # Basic cleaning per the assignment's "meaningful transformation" requirement:
        # drop malformed rows (nulls from bad JSON) and clamp obviously bad readings.
        .filter(F.col("meter_id").isNotNull() & F.col("household_id").isNotNull())
        .withColumn("power_consumption_kwh", F.greatest(F.col("power_consumption_kwh"), F.lit(0.0)))
        .withColumn("solar_generation_kwh", F.greatest(F.col("solar_generation_kwh"), F.lit(0.0)))
    )
    return parsed


def write_zone_window_batch(batch_df: DataFrame, batch_id: int) -> None:
    """foreachBatch sink: upsert windowed zone aggregates into Postgres and
    raise a low-renewable alert row where applicable."""
    if batch_df.rdd.isEmpty():
        return

    rows = batch_df.collect()
    import psycopg2

    conn = psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)
    try:
        with conn.cursor() as cur:
            for r in rows:
                renewable_share = (
                    min(1.0, r["total_solar_kwh"] / r["total_consumption_kwh"])
                    if r["total_consumption_kwh"] and r["total_consumption_kwh"] > 0
                    else (1.0 if r["total_solar_kwh"] > 0 else 0.0)
                )
                cur.execute(
                    """
                    INSERT INTO realtime_zone_stats
                        (grid_zone, window_start, window_end, total_consumption_kwh,
                         total_solar_kwh, renewable_share, reading_count, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, now())
                    ON CONFLICT (grid_zone, window_start) DO UPDATE SET
                        window_end = EXCLUDED.window_end,
                        total_consumption_kwh = EXCLUDED.total_consumption_kwh,
                        total_solar_kwh = EXCLUDED.total_solar_kwh,
                        renewable_share = EXCLUDED.renewable_share,
                        reading_count = EXCLUDED.reading_count,
                        updated_at = now()
                    """,
                    (r["grid_zone"], r["window_start"], r["window_end"],
                     float(r["total_consumption_kwh"]), float(r["total_solar_kwh"]),
                     float(renewable_share), int(r["reading_count"])),
                )

                if renewable_share < LOW_RENEWABLE_THRESHOLD:
                    # "update" output mode re-emits a window on every trigger
                    # until the watermark closes it, so only alert once per
                    # (zone, window) rather than once per micro-batch.
                    cur.execute(
                        """
                        INSERT INTO alerts (alert_type, severity, grid_zone, household_id, window_start, message, created_at)
                        SELECT 'LOW_RENEWABLE_CONTRIBUTION', 'WARNING', %s, NULL, %s, %s, now()
                        WHERE NOT EXISTS (
                            SELECT 1 FROM alerts
                            WHERE alert_type = 'LOW_RENEWABLE_CONTRIBUTION'
                              AND grid_zone = %s AND window_start = %s
                        )
                        """,
                        (r["grid_zone"], r["window_start"],
                         f"Renewable share {renewable_share:.1%} in {r['grid_zone']} "
                         f"is below the {LOW_RENEWABLE_THRESHOLD:.0%} threshold "
                         f"for window {r['window_start']}",
                         r["grid_zone"], r["window_start"]),
                    )
                    if cur.rowcount:
                        log("processing", "low_renewable_alert", grid_zone=r["grid_zone"],
                            renewable_share=round(renewable_share, 4), window_start=str(r["window_start"]))
        conn.commit()
    finally:
        conn.close()

    log("processing", "zone_window_batch_written", batch_id=batch_id, rows=len(rows))


def write_household_latest_batch(batch_df: DataFrame, batch_id: int) -> None:
    """foreachBatch sink: upsert the latest reading per household (used by
    the 'current status' part of the household endpoint and by the
    freshness/health-check alert rule in the API)."""
    if batch_df.rdd.isEmpty():
        return
    rows = batch_df.collect()
    import psycopg2

    conn = psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)
    try:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(
                    """
                    INSERT INTO realtime_household_latest
                        (household_id, meter_id, grid_zone, power_consumption_kwh,
                         solar_generation_kwh, reading_timestamp, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, now())
                    ON CONFLICT (household_id) DO UPDATE SET
                        meter_id = EXCLUDED.meter_id,
                        grid_zone = EXCLUDED.grid_zone,
                        power_consumption_kwh = EXCLUDED.power_consumption_kwh,
                        solar_generation_kwh = EXCLUDED.solar_generation_kwh,
                        reading_timestamp = EXCLUDED.reading_timestamp,
                        updated_at = now()
                    WHERE EXCLUDED.reading_timestamp >= realtime_household_latest.reading_timestamp
                    """,
                    (r["household_id"], r["meter_id"], r["grid_zone"],
                     float(r["power_consumption_kwh"]), float(r["solar_generation_kwh"]),
                     r["event_time"]),
                )
        conn.commit()
    finally:
        conn.close()
    log("processing", "household_latest_batch_written", batch_id=batch_id, rows=len(rows))


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    log("startup", "streaming_job_starting", kafka=KAFKA_BOOTSTRAP_SERVERS, topic=METER_TOPIC)

    events = read_meter_stream(spark).withWatermark("event_time", "2 minutes")

    # --- 1. Raw archive to Parquet (feeds the batch layer) ---------------
    # Partitioned by the *simulated* day the producer stamped on each event
    # (not the wall-clock date), so the batch layer's sim_date matches the
    # tariff_<sim_date>.csv file it joins against.
    raw_query = (
        events.select("meter_id", "household_id", "power_consumption_kwh", "solar_generation_kwh",
                       "grid_zone", "event_time", "sim_date")
        .withColumn("sim_date", F.coalesce(F.col("sim_date"), F.date_format("event_time", "yyyyMMdd")))
        .writeStream.format("parquet")
        .option("path", RAW_ARCHIVE_DIR)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/raw_archive")
        .partitionBy("sim_date", "grid_zone")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # --- 2. Windowed zone aggregation (speed-layer "right now" view) -----
    zone_window = (
        events.groupBy(F.window(F.col("event_time"), "1 minute", "30 seconds"), F.col("grid_zone"))
        .agg(
            F.sum("power_consumption_kwh").alias("total_consumption_kwh"),
            F.sum("solar_generation_kwh").alias("total_solar_kwh"),
            F.count("*").alias("reading_count"),
        )
        .select(
            F.col("grid_zone"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "total_consumption_kwh", "total_solar_kwh", "reading_count",
        )
    )
    zone_query = (
        zone_window.writeStream.outputMode("update")
        .foreachBatch(write_zone_window_batch)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/zone_window")
        .trigger(processingTime="15 seconds")
        .start()
    )

    # --- 3. Latest reading per household (point-lookup view) -------------
    household_latest_query = (
        events.writeStream.outputMode("append")
        .foreachBatch(write_household_latest_batch)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/household_latest")
        .trigger(processingTime="10 seconds")
        .start()
    )

    log("startup", "streaming_job_started", queries=3)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
